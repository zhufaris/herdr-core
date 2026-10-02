use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::PathBuf;

use crate::api::schema::{PaneIdentityReconcileReceipt, PaneIdentityReconcileV1Params};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum PersistedOperationResult {
    Pending,
    Completed {
        receipt: PaneIdentityReconcileReceipt,
    },
    Rejected {
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct PersistedOperation {
    request: PaneIdentityReconcileV1Params,
    result: PersistedOperationResult,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct PersistedJournal {
    operations: BTreeMap<String, PersistedOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OperationState {
    New,
    Pending,
    Completed(PaneIdentityReconcileReceipt),
    Rejected { code: String, message: String },
    Conflict,
}

pub(crate) struct PaneIdentityJournal {
    path: PathBuf,
    persisted: PersistedJournal,
}

impl PaneIdentityJournal {
    pub(crate) fn open(path: PathBuf) -> io::Result<Self> {
        let persisted = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => PersistedJournal::default(),
            Err(err) => return Err(err),
        };
        Ok(Self { path, persisted })
    }

    pub(crate) fn begin(
        &mut self,
        request: &PaneIdentityReconcileV1Params,
    ) -> io::Result<OperationState> {
        if let Some(operation) = self.persisted.operations.get(&request.operation_id) {
            if operation.request != *request {
                return Ok(OperationState::Conflict);
            }
            return Ok(match &operation.result {
                PersistedOperationResult::Pending => OperationState::Pending,
                PersistedOperationResult::Completed { receipt } => {
                    OperationState::Completed(receipt.clone())
                }
                PersistedOperationResult::Rejected { code, message } => OperationState::Rejected {
                    code: code.clone(),
                    message: message.clone(),
                },
            });
        }

        self.persisted.operations.insert(
            request.operation_id.clone(),
            PersistedOperation {
                request: request.clone(),
                result: PersistedOperationResult::Pending,
            },
        );
        self.save()?;
        Ok(OperationState::New)
    }

    pub(crate) fn complete(
        &mut self,
        request: &PaneIdentityReconcileV1Params,
        receipt: PaneIdentityReconcileReceipt,
    ) -> io::Result<()> {
        let operation = self.matching_operation_mut(request)?;
        if let PersistedOperationResult::Completed { receipt: existing } = &operation.result {
            if existing == &receipt {
                return Ok(());
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "pane identity operation has a different receipt",
            ));
        }
        operation.result = PersistedOperationResult::Completed { receipt };
        self.save()
    }

    pub(crate) fn reject(
        &mut self,
        request: &PaneIdentityReconcileV1Params,
        code: &str,
        message: &str,
    ) -> io::Result<()> {
        let operation = self.matching_operation_mut(request)?;
        operation.result = PersistedOperationResult::Rejected {
            code: code.into(),
            message: message.into(),
        };
        self.save()
    }

    fn matching_operation_mut(
        &mut self,
        request: &PaneIdentityReconcileV1Params,
    ) -> io::Result<&mut PersistedOperation> {
        let operation = self
            .persisted
            .operations
            .get_mut(&request.operation_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "operation not found"))?;
        if operation.request != *request {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "operation id was reused with a different request",
            ));
        }
        Ok(operation)
    }

    fn save(&self) -> io::Result<()> {
        let parent = self.path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "journal path has no parent")
        })?;
        std::fs::create_dir_all(parent)?;
        let temporary = self.path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(&self.persisted)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, &self.path)?;
        if let Ok(directory) = std::fs::File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok(())
    }
}

pub(crate) fn default_path() -> PathBuf {
    #[cfg(test)]
    {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let epoch_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!(
            "herdr-pane-identity-{}-{epoch_nanos}-{}.json",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }
    #[cfg(not(test))]
    crate::session::data_dir().join("pane-identity-operations.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::{
        PaneIdentity, PaneIdentityReconcileDisposition, PaneIdentityReconcileTarget,
    };

    fn request(operation_id: &str) -> PaneIdentityReconcileV1Params {
        PaneIdentityReconcileV1Params {
            operation_id: operation_id.into(),
            expected: PaneIdentity {
                pane_id: "w1:p1".into(),
                terminal_id: "term_1".into(),
                workspace_id: "w1".into(),
                token: "old1".into(),
            },
            target: PaneIdentityReconcileTarget {
                workspace_id: "w2".into(),
                expected_workspace_label: "herdr".into(),
                token: "orch".into(),
            },
        }
    }

    fn receipt(request: &PaneIdentityReconcileV1Params) -> PaneIdentityReconcileReceipt {
        PaneIdentityReconcileReceipt {
            operation_id: request.operation_id.clone(),
            disposition: PaneIdentityReconcileDisposition::Applied,
            previous: request.expected.clone(),
            current: PaneIdentity {
                pane_id: "w2:p2".into(),
                terminal_id: request.expected.terminal_id.clone(),
                workspace_id: request.target.workspace_id.clone(),
                token: request.target.token.clone(),
            },
        }
    }

    #[test]
    fn journal_reopens_with_the_same_receipt_and_rejects_mismatched_reuse() {
        let path = default_path();
        let request = request("identity-reopen");
        let receipt = receipt(&request);
        let mut journal = PaneIdentityJournal::open(path.clone()).unwrap();
        assert_eq!(journal.begin(&request).unwrap(), OperationState::New);
        journal.complete(&request, receipt.clone()).unwrap();
        drop(journal);

        let mut reopened = PaneIdentityJournal::open(path.clone()).unwrap();
        assert_eq!(
            reopened.begin(&request).unwrap(),
            OperationState::Completed(receipt)
        );
        let mut mismatched = request;
        mismatched.target.token = "or01".into();
        assert_eq!(
            reopened.begin(&mismatched).unwrap(),
            OperationState::Conflict
        );
        let _ = std::fs::remove_file(path);
    }
}
