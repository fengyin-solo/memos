package objectcleanup

import (
	"fmt"
	"os"

	"github.com/usememos/memos/internal/random"
)

// defaultWorkerID identifies lease owners across processes. Hostname and pid
// distinguish machines and processes; the random suffix disambiguates workers
// inside one process (e.g. tests).
func defaultWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "memos"
	}
	return fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), random.UUID()[:8])
}
