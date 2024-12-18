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

use std::{collections::{HashMap, HashSet}, hash::{DefaultHasher, Hash, Hasher}};
use commands::CommandHandler;
use queries::QueryHandler;
use url::Url as baseUrl;
use chrono::Local;

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
    // store slug creation events as vector
    url_events: Vec<ShortLink>, 
    // store redirect events as hashmap of vectors to split events by slugs, we can do it because all slugs are unique, so we can speed up process of calculating stats
    redirect_events_by_slug: HashMap<String, Vec<Slug>>
}

impl UrlShortenerService {
    /// Creates a new instance of the service
    pub fn new() -> Self {
        Self {
            url_events: Vec::new(),
            redirect_events_by_slug: HashMap::new(),
        }
    }

    fn log(&self, message: String) {
        let now = Local::now().format("%Y-%m-%d %H:%M:%S");
        println!("[{}] {message}", now);
    }
}

impl commands::CommandHandler for UrlShortenerService {
    fn handle_create_short_link(
        &mut self,
        url: Url,
        slug: Option<Slug>,
    ) -> Result<ShortLink, ShortenerError> {
        if baseUrl::parse(&url.0).is_err() {
            return Err(ShortenerError::InvalidUrl);
        }
        
        // We need to process all created slugs and make sure that our new slug doesn't match any of existing slugs
        // It is equal to finding out if we already processed url because we can have only one slug for url
        if self.url_events.iter().any(|event| event.url == url) {
            self.log(format!("Failed to create short link: URL {url:?} already exists"));
            return Err(ShortenerError::SlugAlreadyInUse);
        }

        // Collect all slugs in HashSet to provide existance of slug check O(1)
        let existing_slugs: HashSet<_> = self.url_events.iter().map(|event| &event.slug.0).collect();
        
        // Function that generates slug using hash of url
        fn generate_slug_from_url(url: &str) -> String {
            let mut hasher = DefaultHasher::new();
            url.hash(&mut hasher);
            let hash = hasher.finish();
        
            format!("{:x}", hash).chars().take(SLUG_LEN).collect()
        }

        let short_link = match slug {
            Some(slug) => {
                if existing_slugs.contains(&slug.0) {
                    self.log(format!("Failed to create short link: slug {slug:?} is already in use"));
                    return Err(ShortenerError::SlugAlreadyInUse);
                }
                ShortLink { slug: slug, url }
            },
            None => {
                // We will try to create random slug that doesn't exist yet
                loop {
                    let slug_str = generate_slug_from_url(&url.0);
                    if !existing_slugs.contains(&slug_str) {
                        break ShortLink { slug: Slug(slug_str), url };
                    }
                }                
            }
        };

        // Create event for new slug
        self.url_events.push(short_link.clone());
        self.log(format!("Successfully created short link {short_link:?}"));
        return Ok(short_link);
    }

    fn handle_redirect(
        &mut self,
        slug: Slug,
    ) -> Result<ShortLink, ShortenerError> {
        // Check if slug exists via iterating over url_events
        if let Some(url_event) = self.url_events.iter().find(|url_event| url_event.slug == slug) {
            // Ok, we found it, create redirect event
            if let Some(stat) = self.redirect_events_by_slug.get_mut(&slug.0) {
                stat.push(slug.clone());
            } else {
                self.redirect_events_by_slug.insert(slug.0.clone(), vec![slug.clone()]);
            }

            self.log(format!("Handled redirect of slug {slug:?}"));

            return Ok(ShortLink {
                url: url_event.url.clone(),
                slug,
            });
        }

        self.log(format!("Failed to handle redirect of slug {slug:?}: slug not found"));
        Err(ShortenerError::SlugNotFound)
    }
}

impl queries::QueryHandler for UrlShortenerService {
    fn get_stats(&self, slug: Slug) -> Result<Stats, ShortenerError> {
        // Check all link creation events to figure out if slug exists or not
        if let Some(url_event) = self.url_events.iter().find(|url_event| url_event.slug == slug) {
            // Ok, we found registered slug, now we have to count all redirects for this slug
            let redirects = self.redirect_events_by_slug.get(&slug.0)
                .map_or(0, |stat| stat.iter().filter(|&x| x.0 == slug.0).count()) as u64;

            let stats = Stats{link: url_event.clone(), redirects};
            self.log(format!("Retrieved stats {stats:?}"));
            
            return Ok(stats);
        }

        self.log(format!("Failed to retrieve stat of slug {slug:?}: slug not found"));
        Err(ShortenerError::SlugNotFound)
    }
}

fn main() {
    // Create service instance
    let mut service: UrlShortenerService = UrlShortenerService::new();
    let test_url = Url(String::from("http://relap.io/amazing-receipts-worldwide"));

    // Test link creation with no predefined slug - OK
    let short_link = match service.handle_create_short_link(test_url.clone(), None){
        Ok(short_link) => short_link,
        Err(error) => panic!("Failed to create short link for url {:?} with no predefined slug: {:?}", test_url, error),
    };

    // Test link creation for the same url - FAIL, our urls are unique because slug is made from url and unique!
    match service.handle_create_short_link(test_url.clone(), None){
        Ok(_) => panic!("Something went wrong, we cannot create slug for url that was already registered in system"),
        Err(error) => assert_eq!(error, ShortenerError::SlugAlreadyInUse),
    };

    let test_url = Url(String::from("http://relap.io/amazing-receipts-worldwide1"));
    let test_slug = Some(Slug(String::from("random_slug")));

    // Test link creation with predefined slug - OK
    let short_link_with_slug = match service.handle_create_short_link(test_url.clone(), test_slug.clone()){
        Ok(short_link) => short_link,
        Err(error) => panic!("Failed to create short link for url {:?} with predefined slug {:?}: {:?}", test_url, test_slug.unwrap(), error),
    };

    // Test link creation with predefined slug - FAIL, because we already have this slug!
    match service.handle_create_short_link(Url(String::from("http://relap.io/amazing-receipts-worldwide2")), test_slug.clone()){
        Ok(_) => panic!("Something went wrong, we should have this slug {:?} saved!", test_slug.unwrap()),
        Err(error) => assert_eq!(error, ShortenerError::SlugAlreadyInUse),
    };

    // Test link creation with predefined slug - FAIL, because we already have this url registered!
    match service.handle_create_short_link(test_url.clone(), Some(Slug(String::from("something")))) {
        Ok(_) => panic!("Something went wrong, we should have this slug {:?} saved!", test_slug.unwrap()),
        Err(error) => assert_eq!(error, ShortenerError::SlugAlreadyInUse),
    };

    // Do some redirects for short_link
    let short_link_redirects_count = 10;
    for _ in 0..short_link_redirects_count {
        match service.handle_redirect(short_link.slug.clone()) {
            Ok(link) => assert_eq!(link, short_link),
            Err(error) => panic!("Failed to process redirect of short link {:?}: {:?}!", short_link.slug, error),
        }
    }

    // Do some redirects for slug that doesn't exist in system
    match service.handle_redirect(Slug(String::from("random_slug_1"))) {
        Ok(_) => panic!("We couldn't process redirect of slug {:?} because it doesn't exist", short_link.slug),
        Err(error) => assert_eq!(error, ShortenerError::SlugNotFound),
    }

    // Test retrieving stats for short_link
    match service.get_stats(short_link.slug.clone()) {
        Ok(stats) => assert_eq!(stats.redirects, short_link_redirects_count),
        Err(error) => panic!("Something went wrong while receiving stats for short link {:?}: {:?}", short_link, error),
    }

    // Test retrieving stats for short_link, that doesn't exist
    match service.get_stats(Slug(String::from("random_slug_1"))) {
        Ok(_) => panic!("We shoudn't receive stats for slug that doesn't exist!"),
        Err(error) => assert_eq!(error, ShortenerError::SlugNotFound),
    }
}