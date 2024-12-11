//! ## Task Description
//!
//! The goal is to develop a backend service for shortening URLs using CQRS
//! (Command Query Responsibility Segregation) and ES (Event Sourcing)
//! approaches. The service should support the following features:
//!
//! ## Functional Requirements
//!
//! ### Creating a short link with a random slug
//!
//! The user sends a long URL, and the service returns a shortened URL with a
//! random slug.
//!
//! ### Creating a short link with a predefined slug
//!
//! The user sends a long URL along with a predefined slug, and the service
//! checks if the slug is unique. If it is unique, the service creates the short
//! link.
//!
//! ### Counting the number of redirects for the link
//!
//! - Every time a user accesses the short link, the click count should
//!   increment.
//! - The click count can be retrieved via an API.
//!
//! ### CQRS+ES Architecture
//!
//! CQRS: Commands (creating links, updating click count) are separated from
//! queries (retrieving link information).
//!
//! Event Sourcing: All state changes (link creation, click count update) must be
//! recorded as events, which can be replayed to reconstruct the system's state.
//!
//! ### Technical Requirements
//!
//! - The service must be built using CQRS and Event Sourcing approaches.
//! - The service must be possible to run in Rust Playground (so no database like
//!   Postgres is allowed)
//! - Public API already written for this task must not be changed (any change to
//!   the public API items must be considered as breaking change).
//! - Event Sourcing should be actively utilized for implementing logic, rather
//!   than existing without a clear purpose.

#![allow(unused_variables, dead_code)]

use std::collections::HashSet;
use rand::{distributions::Alphanumeric, Rng};
use url::Url as baseUrl;

const SLUG_LEN: usize = 10;

/// All possible errors of the [`UrlShortenerService`].
#[derive(Debug, PartialEq)]
pub enum ShortenerError {
    /// This error occurs when an invalid [`Url`] is provided for shortening.
    InvalidUrl,

    /// This error occurs when an attempt is made to use a slug (custom alias)
    /// that already exists.
    SlugAlreadyInUse,

    /// This error occurs when the provided [`Slug`] does not map to any existing
    /// short link.
    SlugNotFound,
}

/// A unique string (or alias) that represents the shortened version of the
/// URL.
#[derive(Clone, Debug, PartialEq)]
pub struct Slug(pub String);

/// The original URL that the short link points to.
#[derive(Clone, Debug, PartialEq)]
pub struct Url(pub String);

/// Shortened URL representation.
#[derive(Debug, Clone, PartialEq)]
pub struct ShortLink {
    /// A unique string (or alias) that represents the shortened version of the
    /// URL.
    pub slug: Slug,

    /// The original URL that the short link points to.
    pub url: Url,
}

/// Statistics of the [`ShortLink`].
#[derive(Debug, Clone, PartialEq)]
pub struct Stats {
    /// [`ShortLink`] to which this [`Stats`] are related.
    pub link: ShortLink,

    /// Count of redirects of the [`ShortLink`].
    pub redirects: u64,
}

/// Commands for CQRS.
pub mod commands {
    use super::{ShortLink, ShortenerError, Slug, Url};

    /// Trait for command handlers.
    pub trait CommandHandler {
        /// Creates a new short link. It accepts the original url and an
        /// optional [`Slug`]. If a [`Slug`] is not provided, the service will generate
        /// one. Returns the newly created [`ShortLink`].
        ///
        /// ## Errors
        ///
        /// See [`ShortenerError`].
        fn handle_create_short_link(
            &mut self,
            url: Url,
            slug: Option<Slug>,
        ) -> Result<ShortLink, ShortenerError>;

        /// Processes a redirection by [`Slug`], returning the associated
        /// [`ShortLink`] or a [`ShortenerError`].
        fn handle_redirect(
            &mut self,
            slug: Slug,
        ) -> Result<ShortLink, ShortenerError>;
    }
}

/// Queries for CQRS
pub mod queries {
    use super::{ShortenerError, Slug, Stats};

    /// Trait for query handlers.
    pub trait QueryHandler {
        /// Returns the [`Stats`] for a specific [`ShortLink`], such as the
        /// number of redirects (clicks).
        ///
        /// [`ShortLink`]: super::ShortLink
        fn get_stats(&self, slug: Slug) -> Result<Stats, ShortenerError>;
    }
}

/// CQRS and Event Sourcing-based service implementation
pub struct UrlShortenerService {
    url_events: Vec<ShortLink>,
    stat_events: Vec<Slug>
}

impl UrlShortenerService {
    /// Creates a new instance of the service
    pub fn new() -> Self {
        Self {
            url_events: Vec::new(),
            stat_events: Vec::new(),
        }
    }
}

impl commands::CommandHandler for UrlShortenerService {
    fn handle_create_short_link(
        &mut self,
        url: Url,
        slug: Option<Slug>,
    ) -> Result<ShortLink, ShortenerError> {
        if let Err(_) = baseUrl::parse(&url.0) {
            return Err(ShortenerError::InvalidUrl);
        }
        
        // We need to process all created slugs and make sure that our new slug doesn't match any of existing slugs
        // Let's create HashSet to make O(1) complexity of slug existance check
        let mut collected_slugs = HashSet::new();
        for url_event in &self.url_events {
            collected_slugs.insert(&url_event.slug.0);
        }
        
        let short_link: ShortLink;
        match slug {
            Some(slug) => {
                if collected_slugs.contains(&slug.0) {
                    return Err(ShortenerError::SlugAlreadyInUse);
                }
                short_link = ShortLink{slug, url};
            },
            None => {
                loop {
                    // We will try to generate random slug until it is not present in system yet
                    let slug_str: String = rand::thread_rng().sample_iter(Alphanumeric).take(SLUG_LEN).map(char::from).collect();
                    if !collected_slugs.contains(&slug_str) {
                        short_link = ShortLink{slug: Slug(slug_str), url};
                        break;
                    }
                };
            }
        }

        // Create event for new slug
        self.url_events.push(short_link.clone());

        return Ok(short_link);
    }

    fn handle_redirect(
        &mut self,
        slug: Slug,
    ) -> Result<ShortLink, ShortenerError> {
        // Check all link creation events to figure out if slug exists or not
        for url_event in &self.url_events {
            if url_event.slug == slug {
                self.stat_events.push(slug.clone());
                return Ok(ShortLink{url: url_event.url.clone(), slug})
            }
        }

        return Err(ShortenerError::SlugNotFound);
    }
}

impl queries::QueryHandler for UrlShortenerService {
    fn get_stats(&self, slug: Slug) -> Result<Stats, ShortenerError> {
        // Check all link creation events to figure out if slug exists or not
        for url_event in &self.url_events {
            if url_event.slug == slug {
                // Ok, we found registered slug, now we have to count all redirects for this slug
                let mut redirects: u64 = 0;
                for stat_event in &self.stat_events {
                    if stat_event.0 == slug.0 {
                        redirects += 1;
                    }
                }
                return Ok(Stats{link: url_event.clone(), redirects})
            }
        }

        return Err(ShortenerError::SlugNotFound);
    }
}

fn main() {
}