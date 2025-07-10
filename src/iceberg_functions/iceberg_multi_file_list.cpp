#include "iceberg_multi_file_reader.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_logging.hpp"
#include "iceberg_predicate.hpp"
#include "iceberg_value.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"

#include "metadata/iceberg_predicate_stats.hpp"
#include "metadata/iceberg_table_metadata.hpp"

namespace duckdb {

IcebergMultiFileList::IcebergMultiFileList(ClientContext &context_p, shared_ptr<IcebergScanInfo> scan_info,
                                           const string &path, const IcebergOptions &options)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), context(context_p),
      fs(FileSystem::GetFileSystem(context)), scan_info(scan_info), path(path), lock(), options(options) {
}

string IcebergMultiFileList::ToDuckDBPath(const string &raw_path) {
	return raw_path;
}

string IcebergMultiFileList::GetPath() const {
	return path;
}

const IcebergTableMetadata &IcebergMultiFileList::GetMetadata() const {
	return scan_info->metadata;
}

bool IcebergMultiFileList::HasTransactionData() const {
	return scan_info->transaction_data;
}

const IcebergTransactionData &IcebergMultiFileList::GetTransactionData() const {
	D_ASSERT(HasTransactionData());
	return *scan_info->transaction_data;
}

optional_ptr<IcebergSnapshot> IcebergMultiFileList::GetSnapshot() const {
	return scan_info->snapshot;
}

const IcebergTableSchema &IcebergMultiFileList::GetSchema() const {
	return scan_info->schema;
}

void IcebergMultiFileList::Bind(vector<LogicalType> &return_types, vector<string> &names) {
	lock_guard<mutex> guard(lock);

	if (have_bound) {
		names = this->names;
		return_types = this->types;
		return;
	}

	if (!scan_info) {
		D_ASSERT(!path.empty());
		auto input_string = path;
		auto iceberg_path = IcebergUtils::GetStorageLocation(context, input_string);
		auto iceberg_meta_path = IcebergTableMetadata::GetMetaDataPath(context, iceberg_path, fs, options);
		auto table_metadata = IcebergTableMetadata::Parse(iceberg_meta_path, fs, options.metadata_compression_codec);

		auto temp_data = make_uniq<IcebergScanTemporaryData>();
		temp_data->metadata = IcebergTableMetadata::FromTableMetadata(table_metadata);
		auto &metadata = temp_data->metadata;

		auto found_snapshot = metadata.GetSnapshot(options.snapshot_lookup);
		shared_ptr<IcebergTableSchema> schema;
		if (options.snapshot_lookup.snapshot_source == SnapshotSource::LATEST) {
			schema = metadata.GetSchemaFromId(metadata.current_schema_id);
		} else {
			schema = metadata.GetSchemaFromId(found_snapshot->schema_id);
		}
		scan_info = make_shared_ptr<IcebergScanInfo>(iceberg_path, std::move(temp_data), found_snapshot, *schema);
	}

	if (!initialized) {
		InitializeFiles(guard);
	}

	auto &schema = GetSchema().columns;
	for (auto &schema_entry : schema) {
		names.push_back(schema_entry->name);
		return_types.push_back(schema_entry->type);
	}

	QueryResult::DeduplicateColumns(names);
	for (idx_t i = 0; i < names.size(); i++) {
		schema[i]->name = names[i];
	}

	have_bound = true;
	this->names = names;
	this->types = return_types;
}

unique_ptr<IcebergMultiFileList> IcebergMultiFileList::PushdownInternal(ClientContext &context,
                                                                        TableFilterSet &new_filters) const {
	auto filtered_list = make_uniq<IcebergMultiFileList>(context, scan_info, path, this->options);

	TableFilterSet result_filter_set;

	// Add pre-existing filters
	for (auto &entry : table_filters.filters) {
		result_filter_set.PushFilter(ColumnIndex(entry.first), entry.second->Copy());
	}

	// Add new filters
	for (auto &entry : new_filters.filters) {
		if (entry.first < names.size()) {
			result_filter_set.PushFilter(ColumnIndex(entry.first), entry.second->Copy());
		}
	}

	filtered_list->table_filters = std::move(result_filter_set);
	filtered_list->names = names;
	filtered_list->types = types;
	filtered_list->have_bound = true;
	return filtered_list;
}

unique_ptr<MultiFileList>
IcebergMultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
                                            const vector<string> &names, const vector<LogicalType> &types,
                                            const vector<column_t> &column_ids, TableFilterSet &filters) const {
	if (filters.filters.empty()) {
		return nullptr;
	}

	TableFilterSet filters_copy;
	for (auto &filter : filters.filters) {
		auto column_id = column_ids[filter.first];
		auto previously_pushed_down_filter = this->table_filters.filters.find(column_id);
		if (previously_pushed_down_filter != this->table_filters.filters.end() &&
		    filter.second->Equals(*previously_pushed_down_filter->second)) {
			// Skip filters that we already have pushed down
			continue;
		}
		filters_copy.PushFilter(ColumnIndex(column_id), filter.second->Copy());
	}

	if (!filters_copy.filters.empty()) {
		auto new_snap = PushdownInternal(context, filters_copy);
		return std::move(new_snap);
	}
	return nullptr;
}

unique_ptr<MultiFileList> IcebergMultiFileList::ComplexFilterPushdown(ClientContext &context,
                                                                      const MultiFileOptions &options,
                                                                      MultiFilePushdownInfo &info,
                                                                      vector<unique_ptr<Expression>> &filters) {
	if (filters.empty()) {
		return nullptr;
	}

	FilterCombiner combiner(context);
	for (const auto &filter : filters) {
		combiner.AddFilter(filter->Copy());
	}

	vector<FilterPushdownResult> unused;
	auto filter_set = combiner.GenerateTableScanFilters(info.column_indexes, unused);
	if (filter_set.filters.empty()) {
		return nullptr;
	}

	return PushdownInternal(context, filter_set);
}

vector<OpenFileInfo> IcebergMultiFileList::GetAllFiles() {
	vector<OpenFileInfo> file_list;
	//! Lock is required because it reads the 'data_files' vector
	lock_guard<mutex> guard(lock);
	for (idx_t i = 0; i < data_files.size(); i++) {
		file_list.push_back(GetFileInternal(i, guard));
	}
	return file_list;
}

FileExpandResult IcebergMultiFileList::GetExpandResult() {
	// GetFileInternal(1) will ensure files with index 0 and index 1 are expanded if they are available
	lock_guard<mutex> guard(lock);
	GetFileInternal(1, guard);

	if (data_files.size() > 1) {
		return FileExpandResult::MULTIPLE_FILES;
	} else if (data_files.size() == 1) {
		return FileExpandResult::SINGLE_FILE;
	}

	return FileExpandResult::NO_FILES;
}

idx_t IcebergMultiFileList::GetTotalFileCount() {
	// FIXME: the 'added_files_count' + the 'existing_files_count'
	// in the Manifest List should give us this information without scanning the manifest list
	lock_guard<mutex> guard(lock);

	idx_t i = data_files.size();
	while (!GetFileInternal(i, guard).path.empty()) {
		i++;
	}
	return data_files.size();
}

unique_ptr<NodeStatistics> IcebergMultiFileList::GetCardinality(ClientContext &context) {
	idx_t cardinality = 0;

	if (GetMetadata().iceberg_version == 1) {
		//! We collect no cardinality information from manifests for V1 tables.
		return nullptr;
	}

	//! Make sure we have fetched all manifests
	(void)GetTotalFileCount();

	for (idx_t i = 0; i < data_manifests.size(); i++) {
		cardinality += data_manifests[i].added_rows_count;
		cardinality += data_manifests[i].existing_rows_count;
	}
	for (idx_t i = 0; i < delete_manifests.size(); i++) {
		cardinality -= delete_manifests[i].added_rows_count;
	}
	return make_uniq<NodeStatistics>(cardinality, cardinality);
}

IcebergPredicateStats IcebergPredicateStats::DeserializeBounds(const Value &lower_bound, const Value &upper_bound,
                                                               const string &name, const LogicalType &type) {
	IcebergPredicateStats res;

	if (lower_bound.IsNull()) {
		res.lower_bound = Value(type);
	} else {
		D_ASSERT(lower_bound.type().id() == LogicalTypeId::BLOB);
		auto lower_bound_blob = lower_bound.GetValueUnsafe<string_t>();
		auto deserialized_lower_bound = IcebergValue::DeserializeValue(lower_bound_blob, type);
		if (deserialized_lower_bound.HasError()) {
			throw InvalidConfigurationException("Column %s lower bound deserialization failed: %s", name,
			                                    deserialized_lower_bound.GetError());
		}
		res.lower_bound = deserialized_lower_bound.GetValue();
	}

	if (upper_bound.IsNull()) {
		res.upper_bound = Value(type);
	} else {
		D_ASSERT(upper_bound.type().id() == LogicalTypeId::BLOB);
		auto upper_bound_blob = upper_bound.GetValueUnsafe<string_t>();
		auto deserialized_upper_bound = IcebergValue::DeserializeValue(upper_bound_blob, type);
		if (deserialized_upper_bound.HasError()) {
			throw InvalidConfigurationException("Column %s upper bound deserialization failed: %s", name,
			                                    deserialized_upper_bound.GetError());
		}
		res.upper_bound = deserialized_upper_bound.GetValue();
	}
	return res;
}

bool IcebergMultiFileList::FileMatchesFilter(const IcebergManifestEntry &file) {
	D_ASSERT(!table_filters.filters.empty());

	auto &filters = table_filters.filters;
	auto &schema = GetSchema().columns;

	for (idx_t index = 0; index < schema.size(); index++) {
		auto &column = *schema[index];
		auto it = filters.find(index);

		if (it == filters.end()) {
			continue;
		}
		if (file.lower_bounds.empty() || file.upper_bounds.empty()) {
			//! There are no bounds statistics for the file, can't filter
			continue;
		}

		auto &column_id = column.id;
		auto lower_bound_it = file.lower_bounds.find(column_id);
		auto upper_bound_it = file.upper_bounds.find(column_id);
		Value lower_bound;
		Value upper_bound;
		if (lower_bound_it != file.lower_bounds.end()) {
			lower_bound = lower_bound_it->second;
		}
		if (upper_bound_it != file.upper_bounds.end()) {
			upper_bound = upper_bound_it->second;
		}

		auto stats = IcebergPredicateStats::DeserializeBounds(lower_bound, upper_bound, column.name, column.type);

		int64_t value_count = 0;
		auto value_counts_it = file.value_counts.find(column_id);
		if (value_counts_it != file.value_counts.end()) {
			value_count = value_counts_it->second;
		}

		auto null_counts_it = file.null_value_counts.find(column_id);
		if (null_counts_it != file.null_value_counts.end()) {
			auto &null_counts = null_counts_it->second;
			stats.has_null = null_counts != 0;
			stats.has_not_null = (value_count - null_counts) > 0;
		} else {
			stats.has_not_null = value_count > 0;
		}

		auto nan_counts_it = file.nan_value_counts.find(column_id);
		if (nan_counts_it != file.nan_value_counts.end()) {
			auto &nan_counts = nan_counts_it->second;
			stats.has_nan = nan_counts != 0;
		}

		auto &filter = *it->second;
		if (!IcebergPredicate::MatchBounds(filter, stats, IcebergTransform::Identity())) {
			//! If any predicate fails, exclude the file
			return false;
		}
	}
	return true;
}

optional_ptr<const IcebergManifestEntry> IcebergMultiFileList::GetDataFile(idx_t file_id, lock_guard<mutex> &guard) {
	if (file_id < data_files.size()) {
		//! Have we already scanned this data file and returned it? If so, return it
		return data_files[file_id];
	}

	while (file_id >= data_files.size()) {
		if (current_data_files.empty() || data_file_idx >= current_data_files.size()) {
			current_data_files.clear();
			//! Load the next manifest file
			if (current_data_manifest != data_manifests.end()) {
				auto &manifest = *current_data_manifest;
				auto full_path = options.allow_moved_paths ? IcebergUtils::GetFullPath(path, manifest.manifest_path, fs)
				                                           : manifest.manifest_path;
				auto scan = make_uniq<AvroScan>("IcebergManifest", context, full_path);

				data_manifest_reader->Initialize(std::move(scan));
				data_manifest_reader->SetSequenceNumber(manifest.sequence_number);
				data_manifest_reader->SetPartitionSpecID(manifest.partition_spec_id);

				while (!data_manifest_reader->Finished()) {
					data_manifest_reader->Read(STANDARD_VECTOR_SIZE, current_data_files);
				}
				current_data_manifest++;
			} else if (!transaction_data_manifests.empty()) {
				if (transaction_data_idx >= transaction_data_manifests.size()) {
					//! Exhausted all the transaction-local data
					return nullptr;
				}
				auto &manifest_file = transaction_data_manifests[transaction_data_idx].get();
				auto &data_files = manifest_file.data_files;
				current_data_files.insert(current_data_files.end(), data_files.begin(), data_files.end());
				transaction_data_idx++;
			} else {
				//! No more data manifests to explore
				return nullptr;
			}

			data_file_idx = 0;
		}

		optional_ptr<const IcebergManifestEntry> result;
		while (data_file_idx < current_data_files.size()) {
			auto &data_file = current_data_files[data_file_idx];
			if (!table_filters.filters.empty() && !FileMatchesFilter(data_file)) {
				DUCKDB_LOG(context, IcebergLogType, "Iceberg Filter Pushdown, skipped 'data_file': '%s'",
				           data_file.file_path);
				//! Skip this file
				data_file_idx++;
				continue;
			}

			result = data_file;
			data_file_idx++;
			break;
		}
		if (!result) {
			continue;
		}

		data_files.push_back(*result);
	}

	return data_files[file_id];
}

OpenFileInfo IcebergMultiFileList::GetFileInternal(idx_t file_id, lock_guard<mutex> &guard) {

	if (!initialized) {
		InitializeFiles(guard);
	}

	auto found_data_file = GetDataFile(file_id, guard);
	if (!found_data_file) {
		return OpenFileInfo();
	}

	const auto &data_file = *found_data_file;
	const auto &path = data_file.file_path;

	if (!StringUtil::CIEquals(data_file.file_format, "parquet")) {
		throw NotImplementedException("File format '%s' not supported, only supports 'parquet' currently",
		                              data_file.file_format);
	}

	string file_path = path;
	if (options.allow_moved_paths) {
		auto iceberg_path = GetPath();
		auto &fs = FileSystem::GetFileSystem(context);
		file_path = IcebergUtils::GetFullPath(iceberg_path, path, fs);
	}
	OpenFileInfo res(file_path);
	auto extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	extended_info->options["file_size"] = Value::UBIGINT(data_file.file_size_in_bytes);
	// files managed by Iceberg are never modified - we can keep them cached
	extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	// etag / last modified time can be set to dummy values
	extended_info->options["etag"] = Value("");
	extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	res.extended_info = extended_info;
	return res;
}

OpenFileInfo IcebergMultiFileList::GetFile(idx_t file_id) {
	lock_guard<mutex> guard(lock);
	return GetFileInternal(file_id, guard);
}

static optional_ptr<const TableFilter> GetFilterForColumnIndex(TableFilterSet &filter_set,
                                                               const ColumnIndex &column_index) {
	auto primary_index = column_index.GetPrimaryIndex();
	auto filter_it = filter_set.filters.find(primary_index);
	if (filter_it == filter_set.filters.end()) {
		return nullptr;
	}

	auto &parent_filter = *filter_it->second;
	auto &child_indexes = column_index.GetChildIndexes();

	reference<const TableFilter> current_filter(parent_filter);
	for (idx_t i = 0; i < child_indexes.size(); i++) {
		auto &table_filter = current_filter.get();
		auto &child_index = child_indexes[i];
		auto index = child_index.GetPrimaryIndex();
		if (table_filter.filter_type != TableFilterType::STRUCT_EXTRACT) {
			return nullptr;
		}
		auto &struct_extract = table_filter.Cast<StructFilter>();
		if (struct_extract.child_idx != index) {
			//! This filter is not targeting the column on which a partition exists
			return nullptr;
		}
		current_filter = *struct_extract.child_filter;
	}
	return current_filter.get();
}

bool IcebergMultiFileList::ManifestMatchesFilter(const IcebergManifest &manifest) {
	auto spec_id = manifest.partition_spec_id;
	auto &metadata = GetMetadata();

	auto partition_spec_it = metadata.partition_specs.find(spec_id);
	if (partition_spec_it == metadata.partition_specs.end()) {
		throw InvalidInputException("Manifest %s references 'partition_spec_id' %d which doesn't exist",
		                            manifest.manifest_path, spec_id);
	}
	auto &partition_spec = partition_spec_it->second;
	if (!manifest.partitions.has_partitions) {
		//! No field summaries are present, can't filter anything
		return true;
	}

	auto &field_summaries = manifest.partitions.field_summary;
	if (partition_spec.fields.size() != field_summaries.size()) {
		throw InvalidInputException(
		    "Manifest has %d 'field_summary' entries but the referenced partition spec has %d fields",
		    field_summaries.size(), partition_spec.fields.size());
	}

	if (table_filters.filters.empty()) {
		//! There are no filters
		return true;
	}

	auto &schema = GetSchema().columns;
	unordered_map<uint64_t, ColumnIndex> source_to_column_id;
	IcebergTableSchema::PopulateSourceIdMap(source_to_column_id, schema, nullptr);

	for (idx_t i = 0; i < field_summaries.size(); i++) {
		auto &field_summary = field_summaries[i];
		auto &field = partition_spec.fields[i];

		const auto &column_id = source_to_column_id.at(field.source_id);

		// Find if we have a filter for this source column
		auto table_filter = GetFilterForColumnIndex(table_filters, column_id);
		if (!table_filter) {
			continue;
		}

		auto &column = IcebergTableSchema::GetFromColumnIndex(schema, column_id, 0);
		auto result_type = field.transform.GetSerializedType(column.type);
		auto stats = IcebergPredicateStats::DeserializeBounds(field_summary.lower_bound, field_summary.upper_bound,
		                                                      column.name, result_type);
		stats.has_nan = field_summary.contains_nan;
		stats.has_null = field_summary.contains_null;
		stats.has_not_null = true; // Not enough information in field_summary to determine if this should be false

		if (!IcebergPredicate::MatchBounds(*table_filter, stats, field.transform)) {
			return false;
		}
	}
	return true;
}

void IcebergMultiFileList::InitializeFiles(lock_guard<mutex> &guard) {
	if (initialized) {
		return;
	}
	initialized = true;

	if (scan_info->snapshot) {
		//! Load the snapshot
		auto iceberg_path = GetPath();
		auto &snapshot = *GetSnapshot();
		auto &metadata = GetMetadata();
		auto &fs = FileSystem::GetFileSystem(context);

		data_manifest_reader = make_uniq<manifest_file::ManifestFileReader>(metadata.iceberg_version);
		delete_manifest_reader = make_uniq<manifest_file::ManifestFileReader>(metadata.iceberg_version);

		// Read the manifest list, we need all the manifests to determine if we've seen all deletes
		auto manifest_list_full_path = options.allow_moved_paths
		                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
		                                   : snapshot.manifest_list;

		IcebergManifestList manifest_list(manifest_list_full_path);

		//! Read the manifest list
		auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(metadata.iceberg_version);
		auto scan = make_uniq<AvroScan>("IcebergManifestList", context, manifest_list_full_path);
		manifest_list_reader->Initialize(std::move(scan));
		while (!manifest_list_reader->Finished()) {
			manifest_list_reader->Read(STANDARD_VECTOR_SIZE, manifest_list.manifests);
		}

		for (auto &manifest : manifest_list.manifests) {
			if (!ManifestMatchesFilter(manifest)) {
				DUCKDB_LOG(context, IcebergLogType, "Iceberg Filter Pushdown, skipped 'manifest_file': '%s'",
				           manifest.manifest_path);
				//! Skip this manifest
				continue;
			}

			if (manifest.content == IcebergManifestContentType::DATA) {
				data_manifests.push_back(manifest);
			} else {
				D_ASSERT(manifest.content == IcebergManifestContentType::DELETE);
				delete_manifests.push_back(manifest);
			}
		}
	}

	if (HasTransactionData()) {
		auto &transaction_data = GetTransactionData();
		for (auto &alter_p : transaction_data.alters) {
			auto &alter = alter_p.get();

			if (!ManifestMatchesFilter(alter.manifest)) {
				DUCKDB_LOG(context, IcebergLogType, "Iceberg Filter Pushdown, skipped 'manifest_file': '%s'",
				           alter.manifest.manifest_path);
				//! Skip this manifest
				continue;
			}

			if (alter.snapshot.operation == IcebergSnapshotOperationType::APPEND) {
				transaction_data_manifests.push_back(alter.manifest_file);
			} else {
				throw NotImplementedException("IcebergSnapshotOperationType: %d",
				                              static_cast<uint8_t>(alter.snapshot.operation));
			}
		}
	}

	current_data_manifest = data_manifests.begin();
	current_delete_manifest = delete_manifests.begin();
}

void IcebergMultiFileList::ProcessDeletes(const vector<MultiFileColumnDefinition> &global_columns,
                                          const vector<ColumnIndex> &column_indexes) const {
	// In <=v2 we now have to process *all* delete manifests
	// before we can be certain that we have all the delete data for the current file.

	// v3 solves this, `referenced_data_file` will tell us which file the `data_file`
	// is targeting before we open it, and there can only be one deletion vector per data file.

	// From the spec: "At most one deletion vector is allowed per data file in a snapshot"

	//! NOTE: The lock is required because we're reading from the 'data_files' vector
	auto iceberg_path = GetPath();
	auto &fs = FileSystem::GetFileSystem(context);

	while (current_delete_manifest != delete_manifests.end()) {
		auto &manifest = *current_delete_manifest;
		auto full_path = options.allow_moved_paths ? IcebergUtils::GetFullPath(iceberg_path, manifest.manifest_path, fs)
		                                           : manifest.manifest_path;
		auto scan = make_uniq<AvroScan>("IcebergManifest", context, full_path);

		delete_manifest_reader->Initialize(std::move(scan));
		delete_manifest_reader->SetSequenceNumber(manifest.sequence_number);
		delete_manifest_reader->SetPartitionSpecID(manifest.partition_spec_id);

		IcebergManifestFile manifest_file(full_path);
		while (!delete_manifest_reader->Finished()) {
			delete_manifest_reader->Read(STANDARD_VECTOR_SIZE, manifest_file.data_files);
		}

		current_delete_manifest++;

		for (auto &entry : manifest_file.data_files) {
			if (StringUtil::CIEquals(entry.file_format, "parquet")) {
				ScanDeleteFile(entry, global_columns, column_indexes);
			} else if (StringUtil::CIEquals(entry.file_format, "puffin")) {
				ScanPuffinFile(entry);
			} else {
				throw NotImplementedException(
				    "File format '%s' not supported for deletes, only supports 'parquet' and 'puffin' currently",
				    entry.file_format);
			}
		}
	}

	D_ASSERT(current_delete_manifest == delete_manifests.end());
}

void IcebergMultiFileList::ScanDeleteFile(const IcebergManifestEntry &entry,
                                          const vector<MultiFileColumnDefinition> &global_columns,
                                          const vector<ColumnIndex> &column_indexes) const {
	const auto &delete_file_path = entry.file_path;
	auto &instance = DatabaseInstance::GetDatabase(context);
	//! FIXME: delete files could also be made without row_ids,
	//! in which case we need to rely on the `'schema.column-mapping.default'` property just like data files do.
	auto &parquet_scan_entry = ExtensionUtil::GetTableFunction(instance, "parquet_scan");
	auto &parquet_scan = parquet_scan_entry.functions.functions[0];

	// Prepare the inputs for the bind
	vector<Value> children;
	children.reserve(1);
	children.push_back(Value(delete_file_path));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<string> input_names;

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.name = "IcebergDeleteScan";
	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  dummy_table_function, empty);
	vector<LogicalType> return_types;
	vector<string> return_names;

	auto bind_data = parquet_scan.bind(context, bind_input, return_types, return_names);

	DataChunk result;
	// Reserve for STANDARD_VECTOR_SIZE instead of count, in case the returned table contains too many tuples
	result.Initialize(context, return_types, STANDARD_VECTOR_SIZE);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);

	vector<column_t> column_ids;
	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
	}
	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	auto global_state = parquet_scan.init_global(context, input);
	auto local_state = parquet_scan.init_local(execution_context, input, global_state.get());

	auto &multi_file_local_state = local_state->Cast<MultiFileLocalState>();

	if (entry.content == IcebergManifestEntryContentType::POSITION_DELETES) {
		do {
			TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
			result.Reset();
			parquet_scan.function(context, function_input, result);
			result.Flatten();
			ScanPositionalDeleteFile(result);
		} while (result.size() != 0);
	} else if (entry.content == IcebergManifestEntryContentType::EQUALITY_DELETES) {
		do {
			TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
			result.Reset();
			parquet_scan.function(context, function_input, result);
			result.Flatten();
			ScanEqualityDeleteFile(entry, result, multi_file_local_state.reader->columns, global_columns,
			                       column_indexes);
		} while (result.size() != 0);
	}
}

//! FIXME: isn't this problematic if we need to scan the same delete file multiple times??
unique_ptr<DeleteFilter> IcebergMultiFileList::GetPositionalDeletesForFile(const string &file_path) const {
	auto it = positional_delete_data.find(file_path);
	if (it != positional_delete_data.end()) {
		// There is delete data for this file, return it
		return std::move(it->second);
	}
	return nullptr;
}

} // namespace duckdb
