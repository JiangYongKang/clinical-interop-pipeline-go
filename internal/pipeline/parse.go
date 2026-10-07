package pipeline

import (
	"time"

	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/domain"
	"github.com/highcumontoa/clinical-interop-pipeline-go/internal/ingest"
)

// parseOne wraps ingest.Parse so batch and single ingestion share one entry.
func parseOne(format string, payload []byte, now time.Time) (*ingest.RawDocument, *domain.RejectError) {
	doc, err := ingest.Parse(format, payload, now)
	if err != nil {
		return nil, asReject(err)
	}
	return doc, nil
}
