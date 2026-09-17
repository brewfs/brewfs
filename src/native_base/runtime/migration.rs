use super::{NativeRuntimeCapabilities, NativeVolumeHeader, RuntimeAdmissionError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationMode {
    NewNamespace,
    OfflineCopy,
    InPlaceAdopt,
    OnlineHeaderRewrite,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationRequest<'a> {
    pub mode: MigrationMode,
    pub source_namespace: Option<&'a str>,
    pub target_namespace: &'a str,
    pub source_quiesced: bool,
    pub header: &'a NativeVolumeHeader,
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("offline copy requires a quiesced source")]
    SourceNotQuiesced,
    #[error("source and target namespaces must be different")]
    NamespaceReuse,
    #[error("offline copy requires an explicit source namespace")]
    MissingSource,
    #[error("in-place adoption is not implemented; use a new namespace or offline copy")]
    InPlaceAdoptUnsupported,
    #[error("online native header rewrite is forbidden")]
    OnlineHeaderRewriteForbidden,
    #[error(transparent)]
    Admission(#[from] RuntimeAdmissionError),
}

pub fn validate_migration(request: &MigrationRequest<'_>) -> Result<(), MigrationError> {
    request
        .header
        .validate(NativeRuntimeCapabilities::compiled())?;
    match request.mode {
        MigrationMode::NewNamespace => {
            if request.source_namespace.is_some() {
                return Err(MigrationError::NamespaceReuse);
            }
            Ok(())
        }
        MigrationMode::OfflineCopy => {
            let source = request
                .source_namespace
                .ok_or(MigrationError::MissingSource)?;
            if source == request.target_namespace {
                return Err(MigrationError::NamespaceReuse);
            }
            if !request.source_quiesced {
                return Err(MigrationError::SourceNotQuiesced);
            }
            Ok(())
        }
        MigrationMode::InPlaceAdopt => Err(MigrationError::InPlaceAdoptUnsupported),
        MigrationMode::OnlineHeaderRewrite => Err(MigrationError::OnlineHeaderRewriteForbidden),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(mode: MigrationMode) -> MigrationRequest<'static> {
        let header = Box::leak(Box::new(NativeVolumeHeader::p1([1; 16], [2; 16])));
        MigrationRequest {
            mode,
            source_namespace: Some("source"),
            target_namespace: "target",
            source_quiesced: true,
            header,
        }
    }

    #[cfg(feature = "native-packed-base")]
    #[test]
    fn only_new_namespace_and_quiesced_offline_copy_are_admitted() {
        let mut new = request(MigrationMode::NewNamespace);
        new.source_namespace = None;
        validate_migration(&new).unwrap();
        validate_migration(&request(MigrationMode::OfflineCopy)).unwrap();

        let mut live = request(MigrationMode::OfflineCopy);
        live.source_quiesced = false;
        assert!(matches!(
            validate_migration(&live),
            Err(MigrationError::SourceNotQuiesced)
        ));
        assert!(matches!(
            validate_migration(&request(MigrationMode::InPlaceAdopt)),
            Err(MigrationError::InPlaceAdoptUnsupported)
        ));
        assert!(matches!(
            validate_migration(&request(MigrationMode::OnlineHeaderRewrite)),
            Err(MigrationError::OnlineHeaderRewriteForbidden)
        ));
    }
}
