use crate::quorum::Index;
use std::cmp::max;

/// Updates the top 2 grouped indices with a new index to maintain the order.
/// if group ID is 0, it means this position is initialized.
pub(crate) fn update_top2_grouped_index(last: &mut [Index; 2], new: Index) {
    if last[0].group_id == 0 {
        last[0] = new;
        return;
    }
    if last[0].group_id == new.group_id {
        last[0].index = max(last[0].index, new.index);
        return;
    }

    // above are cases that only have one element

    if last[1].group_id == 0 {
        last[1] = new;
        if last[0].index < last[1].index {
            last.swap(0, 1);
        }
        return;
    }
    if last[1].group_id == new.group_id {
        last[1].index = max(last[1].index, new.index);
        if last[0].index < last[1].index {
            last.swap(0, 1);
        }
        return;
    }

    // now 3 elements are all different

    if last[0].index < new.index {
        last[1] = last[0];
        last[0] = new;
    } else if last[1].index < new.index {
        last[1] = new;
    }
}
