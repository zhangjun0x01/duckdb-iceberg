#include "storage/irc_catalog.hpp"
#include "storage/irc_schema_entry.hpp"
#include "storage/irc_table_entry.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "catalog_api.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "storage/authorization/sigv4.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

#include "rest_catalog/objects/list.hpp"
#include "storage/irc_transaction.hpp"

namespace duckdb {

ICTableEntry::ICTableEntry(IcebergTableInformation &table_info, Catalog &catalog, SchemaCatalogEntry &schema,
                           CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info), table_info(table_info) {
	this->internal = false;
}

unique_ptr<BaseStatistics> ICTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void ICTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                         ClientContext &) {
	throw NotImplementedException("BindUpdateConstraints");
}

string ICTableEntry::PrepareIcebergScanFromEntry(ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	auto &secret_manager = SecretManager::Get(context);

	// Get Credentials from IRC API
	auto table_credentials = table_info.GetVendedCredentials(context);
	auto &load_result = table_info.load_table_result;

	if (table_credentials.config) {
		auto &info = *table_credentials.config;
		D_ASSERT(info.scope.empty());
		//! Limit the scope to the metadata location
		string lc_storage_location = StringUtil::Lower(load_result.metadata_location);
		size_t metadata_pos = lc_storage_location.find("metadata");
		if (metadata_pos != string::npos) {
			info.scope = {load_result.metadata_location.substr(0, metadata_pos)};
		} else {
			throw InvalidInputException("Substring not found");
		}

		if (StringUtil::StartsWith(ic_catalog.uri, "glue")) {
			auto &sigv4_auth = ic_catalog.auth_handler->Cast<SIGV4Authorization>();
			//! Override the endpoint if 'glue' is the host of the catalog
			auto secret_entry = IRCatalog::GetStorageSecret(context, sigv4_auth.secret);
			auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
			auto region = kv_secret.TryGetValue("region").ToString();
			auto endpoint = "s3." + region + ".amazonaws.com";
			info.options["endpoint"] = endpoint;
		} else if (StringUtil::StartsWith(ic_catalog.uri, "s3tables")) {
			auto &sigv4_auth = ic_catalog.auth_handler->Cast<SIGV4Authorization>();
			//! Override all the options if 's3tables' is the host of the catalog
			auto secret_entry = IRCatalog::GetStorageSecret(context, sigv4_auth.secret);
			auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
			auto substrings = StringUtil::Split(ic_catalog.warehouse, ":");
			D_ASSERT(substrings.size() == 6);
			auto region = substrings[3];
			auto endpoint = "s3." + region + ".amazonaws.com";
			info.options = {{"key_id", kv_secret.TryGetValue("key_id").ToString()},
			                {"secret", kv_secret.TryGetValue("secret").ToString()},
			                {"session_token", kv_secret.TryGetValue("session_token").IsNull()
			                                      ? ""
			                                      : kv_secret.TryGetValue("session_token").ToString()},
			                {"region", region},
			                {"endpoint", endpoint}};
		}

		(void)secret_manager.CreateSecret(context, info);
	}

	for (auto &info : table_credentials.storage_credentials) {
		(void)secret_manager.CreateSecret(context, info);
	}
	return table_info.load_table_result.metadata_location;
}

TableFunction ICTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                            const EntryLookupInfo &lookup) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &iceberg_scan_function_set = ExtensionUtil::GetTableFunction(db, "iceberg_scan");
	auto iceberg_scan_function =
	    iceberg_scan_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});
	auto storage_location = PrepareIcebergScanFromEntry(context);

	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;

	auto &metadata = table_info.table_metadata;
	auto at = lookup.GetAtClause();
	auto snapshot_lookup = IcebergSnapshotLookup::FromAtClause(at);
	auto snapshot = metadata.GetSnapshot(snapshot_lookup);

	int32_t schema_id;
	if (snapshot_lookup.IsLatest()) {
		schema_id = metadata.current_schema_id;
	} else {
		D_ASSERT(snapshot);
		schema_id = snapshot->schema_id;
	}

	auto schema = metadata.GetSchemaFromId(schema_id);
	auto scan_info =
	    make_shared_ptr<IcebergScanInfo>(table_info.load_table_result.metadata_location, metadata, snapshot, *schema);
	if (table_info.transaction_data) {
		scan_info->transaction_data = table_info.transaction_data.get();
	}

	iceberg_scan_function.function_info = scan_info;

	// Set the S3 path as input to table function
	vector<Value> inputs = {storage_location};
	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, iceberg_scan_function,
	                                  empty_ref);
	auto result = iceberg_scan_function.bind(context, bind_input, return_types, names);
	bind_data = std::move(result);
	return iceberg_scan_function;
}

TableFunction ICTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	throw InternalException("ICTableEntry::GetScanFunction called without entry lookup info");
}

virtual_column_map_t ICTableEntry::GetVirtualColumns() const {
	virtual_column_map_t result;
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
	result.insert(
	    make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX, TableColumn("file_index", LogicalType::UBIGINT)));
	result.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
	return result;
}

vector<column_t> ICTableEntry::GetRowIdColumns() const {
	vector<column_t> result;
	result.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX);
	result.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
	return result;
}

TableStorageInfo ICTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	// TODO fill info
	return result;
}

} // namespace duckdb
