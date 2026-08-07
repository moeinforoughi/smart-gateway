package domain

// IngestResult reports what a node refresh cycle changed. It is produced by
// the persistence layer and consumed by orchestration services.
type IngestResult struct {
	Added   int
	Updated int
	Skipped int
}
