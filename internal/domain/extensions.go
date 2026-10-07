package domain

// New disposition(s) added for atomic batch ingestion.
const (
	// StatusRejectedBatch means a batch cannot be committed as a whole:
	// its batch id was reused with different content, or some of its
	// messages were already committed outside this batch, so committing
	// it would create a partial / non-reproducible state.
	StatusRejectedBatch Status = "rejected_batch"
)
