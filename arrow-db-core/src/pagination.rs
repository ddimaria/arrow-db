//! Pagination utilities for query results.
//!
//! Provides pagination metadata for DataFrame-based queries.

use serde::{Deserialize, Serialize};

/// Pagination metadata returned with query results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginationInfo {
    /// Current page number (0-indexed)
    pub page: usize,
    /// Number of rows per page
    pub page_size: usize,
    /// Number of rows returned in this page
    pub rows_in_page: usize,
    /// Total number of rows (if computed)
    pub total_rows: Option<usize>,
    /// Total number of pages (if total_rows is known)
    pub total_pages: Option<usize>,
    /// Whether there is a next page
    pub has_next_page: bool,
    /// Whether there is a previous page
    pub has_previous_page: bool,
}

impl PaginationInfo {
    /// Create pagination info from query results
    pub fn new(
        page: usize,
        page_size: usize,
        rows_in_page: usize,
        total_rows: Option<usize>,
    ) -> Self {
        let total_pages = total_rows.map(|total| total.div_ceil(page_size));
        let has_next_page = match total_rows {
            Some(total) => (page + 1) * page_size < total,
            None => rows_in_page == page_size, // If we got a full page, assume there might be more
        };
        let has_previous_page = page > 0;

        PaginationInfo {
            page,
            page_size,
            rows_in_page,
            total_rows,
            total_pages,
            has_next_page,
            has_previous_page,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagination_info() {
        let info = PaginationInfo::new(0, 10, 10, Some(25));
        assert_eq!(info.page, 0);
        assert_eq!(info.page_size, 10);
        assert_eq!(info.rows_in_page, 10);
        assert_eq!(info.total_rows, Some(25));
        assert_eq!(info.total_pages, Some(3));
        assert!(info.has_next_page);
        assert!(!info.has_previous_page);

        let info_last_page = PaginationInfo::new(2, 10, 5, Some(25));
        assert!(!info_last_page.has_next_page);
        assert!(info_last_page.has_previous_page);
    }

    #[test]
    fn test_pagination_info_without_total() {
        // When total is unknown, assume there's more if we got a full page
        let info_full_page = PaginationInfo::new(1, 10, 10, None);
        assert!(info_full_page.has_next_page);
        assert!(info_full_page.has_previous_page);

        let info_partial_page = PaginationInfo::new(1, 10, 7, None);
        assert!(!info_partial_page.has_next_page);
        assert!(info_partial_page.has_previous_page);
    }
}
