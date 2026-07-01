use std::cmp::{max, min};

pub(crate) struct Intervals<T: Copy + Ord>(Vec<(T, T)>);

impl<T: Copy + Ord> Intervals<T> {
    pub(crate) fn new(l: T, r: T) -> Self {
        debug_assert!(l <= r, "invalid interval: left must be <= right");
        Intervals(vec![(l, r)])
    }

    pub(crate) fn cut(&mut self, slice_l: T, slice_r: T) -> Vec<(T, T)> {
        let mut cut = Vec::new();
        self.cut_each(slice_l, slice_r, |l, r| cut.push((l, r)));
        cut
    }

    pub(crate) fn cut_each<F>(&mut self, slice_l: T, slice_r: T, mut on_cut: F) -> bool
    where
        F: FnMut(T, T),
    {
        if self.0.is_empty() {
            return false;
        }

        let mut remaining = Vec::with_capacity(self.0.len() + 1);
        let mut touched = false;

        for &(l, r) in &self.0 {
            if r <= slice_l || l >= slice_r {
                remaining.push((l, r));
                continue;
            }

            touched = true;
            let cut_l = max(l, slice_l);
            let cut_r = min(r, slice_r);
            if cut_l < cut_r {
                on_cut(cut_l, cut_r);
            }
            if l < cut_l {
                remaining.push((l, cut_l));
            }
            if cut_r < r {
                remaining.push((cut_r, r));
            }
        }

        if !touched {
            return false;
        }

        self.0 = remaining;
        true
    }

    pub(crate) fn collect(self) -> Vec<(T, T)> {
        self.0
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
