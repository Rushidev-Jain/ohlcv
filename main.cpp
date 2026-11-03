#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>

auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
arrow::dataset::FileSystemFactoryOptions options;
auto factory = arrow::dataset::FileSystemDatasetFactory::Make(
    fs, {"database/raw"}, std::make_shared<arrow::dataset::ParquetFileFormat>(), options
).ValueOrDie();
auto dataset = factory->Finish().ValueOrDie();
auto scanner_builder = dataset->NewScan().ValueOrDie();
scanner_builder->Filter(arrow::compute::equal(
    arrow::compute::field_ref("Symbol"), arrow::compute::literal("AAPL")));
auto table = scanner_builder->Finish().ValueOrDie()->ToTable().ValueOrDie();