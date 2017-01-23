package azureblobstorage

import (
	"testing"
	"github.com/facebookgo/rocks-strata/strata"
)

func TestAzureStorage(t *testing.T) {
	t.Parallel()
	azure := NewMockAzure(t)
	defer azure.Stop()

	strata.HelpTestStorage(t, azure.azureEmulator)
}

func TestAzureStorageManyFiles(t *testing.T) {
	t.Parallel()
	azure := NewMockAzure(t)
	defer azure.Stop()

	strata.HelpTestStorageManyFiles(t, azure.azureEmulator)

}
