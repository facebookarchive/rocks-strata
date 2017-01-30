package azureblobstorage

import (
	"os"
	"testing"

	"github.com/facebookgo/rocks-strata/strata"
)

func TestAzureStorage(t *testing.T) {
	if os.Getenv("USING_AZURE_STORAGE_EMULATOR") != "true" {
		t.Skip("Skipping test. Set USING_AZURE_STORAGE_EMULATOR=true to run.")
	}

	t.Parallel()
	azure := NewMockAzure(t)
	defer azure.Stop()

	strata.HelpTestStorage(t, azure.azureEmulator)
}

func TestAzureStorageManyFiles(t *testing.T) {
	if os.Getenv("USING_AZURE_STORAGE_EMULATOR") != "true" {
		t.Skip("Skipping test. Set USING_AZURE_STORAGE_EMULATOR=true to run.")
	}

	t.Parallel()
	azure := NewMockAzure(t)
	defer azure.Stop()

	strata.HelpTestStorageManyFiles(t, azure.azureEmulator)

}
