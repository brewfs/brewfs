//! In-memory request bodies with shared fragment ownership.
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use http_body_1x::{Body, Frame, SizeHint};
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

struct VectoredBody {
    chunks: std::vec::IntoIter<Bytes>,
    remaining: u64,
}

impl VectoredBody {
    fn new(chunks: Vec<Bytes>) -> Self {
        Self {
            remaining: chunks.iter().map(|chunk| chunk.len() as u64).sum(),
            chunks: chunks.into_iter(),
        }
    }
}

impl Body for VectoredBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        for chunk in self.chunks.by_ref() {
            if !chunk.is_empty() {
                self.remaining -= chunk.len() as u64;
                return Poll::Ready(Some(Ok(Frame::data(chunk))));
            }
        }
        Poll::Ready(None)
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }
}

pub(super) fn retryable_body(chunks: &[Bytes]) -> SdkBody {
    let chunks = chunks.to_vec();
    SdkBody::retryable(move || SdkBody::from_body_1_x(VectoredBody::new(chunks.clone())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_1x::Body;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    fn next(body: &mut SdkBody) -> Option<Bytes> {
        let mut context = Context::from_waker(Waker::noop());
        match Body::poll_frame(Pin::new(body), &mut context) {
            Poll::Ready(Some(Ok(frame))) => Some(frame.into_data().unwrap()),
            Poll::Ready(None) => None,
            other => panic!("in-memory body must be ready: {other:?}"),
        }
    }

    fn assert_frames(body: &mut SdkBody, expected: &[Bytes]) {
        let mut remaining = expected.iter().map(Bytes::len).sum::<usize>();
        for chunk in expected {
            assert_eq!(body.size_hint().exact(), Some(remaining as u64));
            let actual = next(body).unwrap();
            assert_eq!(actual.as_ptr(), chunk.as_ptr(), "payload must stay shared");
            assert_eq!(actual, chunk);
            remaining -= chunk.len();
        }
        assert!(next(body).is_none());
        assert!(body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(0));
    }

    #[test]
    fn fragments_keep_owners_and_retries_restart_after_partial_consumption() {
        let data = Bytes::from(vec![17; 1024 * 1024]);
        let chunks = vec![
            Bytes::from_static(b"SF\0\0"),
            Bytes::new(),
            data.slice(..7),
            data.slice(7..),
            Bytes::new(),
        ];
        let expected = chunks
            .iter()
            .filter(|chunk| !chunk.is_empty())
            .cloned()
            .collect::<Vec<_>>();
        let mut body = retryable_body(&chunks);
        assert!(body.bytes().is_none(), "must not flatten the payload");
        assert_eq!(body.content_length(), Some((data.len() + 4) as u64));
        assert_eq!(next(&mut body).unwrap().as_ptr(), chunks[0].as_ptr());
        let mut retry = body.try_clone().expect("in-memory body must be retryable");
        assert_eq!(retry.content_length(), Some((data.len() + 4) as u64));
        assert_frames(&mut body, &expected[1..]);
        assert_frames(&mut retry, &expected);
    }

    #[test]
    fn retryable_body_releases_its_payload_owners_on_drop() {
        let data = Bytes::from(vec![42; 1024 * 1024]);
        let body = retryable_body(&[data.clone(), Bytes::new()]);
        assert!(!data.is_unique(), "the request must retain the allocation");
        drop(body);
        assert!(data.try_into_mut().is_ok());
    }

    #[test]
    fn empty_fragments_have_an_exact_zero_length() {
        for chunks in [vec![], vec![Bytes::new(), Bytes::new()]] {
            let mut body = retryable_body(&chunks);
            let mut retry = body.try_clone().unwrap();
            assert_eq!(body.content_length(), Some(0));
            assert_frames(&mut body, &[]);
            assert_frames(&mut retry, &[]);
        }
    }
}
