"""
Product Aware API Service - Python Flask Version
Handles interaction with the Product Aware API for product data retrieval.
"""

import requests
import logging
from urllib.parse import quote
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class ProductAwareService:
    """Service for interacting with the Product Aware API"""

    def __init__(self, api_url: str, api_key: str):
        """
        Initialize the Product Aware service

        Args:
            api_url: Base URL for the Product Aware API
            api_key: API key for authentication
        """
        self.api_url = api_url
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': api_key if api_key.startswith('Bearer ') else f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'User-Agent': 'Supply-Chain-Risk-Analysis/1.0'
        })

        # Cache for product data (simple in-memory cache)
        self.cache = {}
        self.cache_expiry = {}
        self.cache_duration = timedelta(hours=1)

        # Fast search cache - stores products by name for quick lookup
        self.fast_search_cache = {}
        self.cache_loaded = False
        self.cache_loading = False

        logger.info(f"Product Aware Service initialized with URL: {api_url}")

        # Start background cache loading
        self._start_background_cache_loading()

    def _start_background_cache_loading(self):
        """Start loading all products in the background"""
        import threading

        def load_cache():
            try:
                logger.info("Starting background cache loading...")
                self.cache_loading = True
                all_products = self._fetch_all_products_sync()

                # Build fast search cache
                for product in all_products:
                    name = product.get('product_name', '').lower()
                    code = product.get('product_code', '') or ''
                    code = code.lower()

                    # Index by name
                    if name:
                        if name not in self.fast_search_cache:
                            self.fast_search_cache[name] = []
                        self.fast_search_cache[name].append(product)

                    # Index by code
                    if code:
                        if code not in self.fast_search_cache:
                            self.fast_search_cache[code] = []
                        self.fast_search_cache[code].append(product)

                self.cache_loaded = True
                self.cache_loading = False
                logger.info(f"Background cache loading complete! Indexed {len(all_products)} products")

            except Exception as e:
                logger.error(f"Background cache loading failed: {str(e)}")
                self.cache_loading = False

        # Start background thread
        thread = threading.Thread(target=load_cache, daemon=True)
        thread.start()

    def _fetch_all_products_sync(self) -> List[Dict[str, Any]]:
        """Synchronously fetch all products (used by background loading)"""
        all_products = []
        page = 1
        max_pages = 20

        while page <= max_pages:
            try:
                url = f"{self.api_url}?page={page}"
                response = self.session.get(url, timeout=30)
                response.raise_for_status()

                data = response.json()
                items = data.get('items', [])

                if not items:
                    break

                all_products.extend(items)

                if len(items) < 100:
                    break

                page += 1

            except Exception as e:
                logger.error(f"Error fetching page {page}: {str(e)}")
                break

        return all_products

    def _is_cache_valid(self, key: str) -> bool:
        """Check if cached data is still valid"""
        if key not in self.cache or key not in self.cache_expiry:
            return False
        return datetime.now() < self.cache_expiry[key]

    def _set_cache(self, key: str, data: Any) -> None:
        """Store data in cache with expiry"""
        self.cache[key] = data
        self.cache_expiry[key] = datetime.now() + self.cache_duration
        logger.debug(f"Cached data for key: {key}")

    def _get_cache(self, key: str) -> Optional[Any]:
        """Retrieve data from cache if valid"""
        if self._is_cache_valid(key):
            logger.debug(f"Cache HIT for key: {key}")
            return self.cache[key]
        logger.debug(f"Cache MISS for key: {key}")
        return None

    def extract_country(self, location_name: str, location_state: str) -> str:
        """
        Extract and normalize country name from location fields

        Args:
            location_name: Full location name (e.g., "Sydney NSW, Australia")
            location_state: State/country field (e.g., " Australia")

        Returns:
            Normalized country name
        """
        # Try location_name first (e.g., "Sydney NSW, Australia")
        if location_name and ',' in location_name:
            parts = location_name.split(',')
            last_part = parts[-1].strip()
            if last_part and last_part != '':
                return last_part

        # Fallback to location_state
        if location_state:
            cleaned = location_state.strip()
            # Normalize common country names
            cleaned = cleaned.replace(' Australia', 'Australia')
            cleaned = cleaned.replace(' China', 'China')
            cleaned = cleaned.replace(' USA', 'USA')
            return cleaned if cleaned else 'Unknown'

        # Try location_name if no comma
        if location_name:
            trimmed = location_name.strip()
            if len(trimmed) > 2 and ',' not in trimmed:
                return trimmed

        return 'Unknown'

    def get_all_products(self, max_pages: int = 20) -> List[Dict[str, Any]]:
        """
        Get all products from the API with pagination

        Args:
            max_pages: Maximum number of pages to fetch

        Returns:
            List of all products
        """
        cache_key = f"all_products_{max_pages}"
        cached_data = self._get_cache(cache_key)
        if cached_data:
            return cached_data

        logger.info(f"🔍 Fetching all products (max {max_pages} pages)")
        all_products = []
        page = 1

        while page <= max_pages:
            try:
                url = f"{self.api_url}?page={page}"
                logger.info(f"📄 Fetching page {page}: {url}")

                response = self.session.get(url, timeout=30)
                response.raise_for_status()

                data = response.json()
                items = data.get('items', [])

                if not items:
                    logger.info(f"📄 Page {page} is empty, stopping pagination")
                    break

                all_products.extend(items)
                logger.info(f"✅ Page {page}: Retrieved {len(items)} products (Total: {len(all_products)})")

                # Check if this is the last page
                if len(items) < 100:
                    logger.info(f"📄 Page {page} has {len(items)} items (< 100), last page reached")
                    break

                page += 1

            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Error fetching page {page}: {str(e)}")
                break
            except Exception as e:
                logger.error(f"❌ Unexpected error on page {page}: {str(e)}")
                break

        logger.info(f"🎉 Retrieved {len(all_products)} total products from {page-1} pages")
        self._set_cache(cache_key, all_products)
        return all_products

    def _add_image_urls(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Add image URL fields to products using AWS S3 URLs

        Images are hosted on AWS S3 (Amazon Simple Storage Service) at:
        https://architectsdeclareapp.s3.amazonaws.com/media/

        This is the same method used by the Product Aware web interface
        and confirmed working in the CODE2120-W8 lifecycle analyzer.

        Args:
            products: List of products with 'image' field containing relative paths

        Returns:
            List of products with 'image_url' and 'thumbnail_url' fields
        """
        for product in products:
            image_path = product.get('image', '')

            if image_path:
                # Convert relative path to full S3 URL
                if image_path.startswith('products/'):
                    image_url = f'https://architectsdeclareapp.s3.amazonaws.com/media/{image_path}'
                elif image_path.startswith('/products/'):
                    image_url = f'https://architectsdeclareapp.s3.amazonaws.com/media{image_path}'
                else:
                    # If it's already a full URL or different format, use as-is
                    image_url = image_path

                product['image_url'] = image_url
                product['thumbnail_url'] = image_url
            else:
                # No image available - frontend will show placeholder
                product['image_url'] = ''
                product['thumbnail_url'] = ''

        return products

    def search_products(self, query: str, max_results: int = 50) -> List[Dict[str, Any]]:
        """
        Search for products by name or code (optimized with fast cache)

        Args:
            query: Search query
            max_results: Maximum number of results to return

        Returns:
            List of matching products
        """
        logger.info(f"Searching products with query: '{query}'")

        query_lower = query.lower()
        matches = []

        # Try fast search first if cache is loaded
        if self.cache_loaded and self.fast_search_cache:
            logger.info("Using fast search cache...")
            matches = self._fast_search(query_lower, max_results)
        else:
            # Fallback to regular search with timeout
            logger.info("Using regular search (cache not ready)...")
            matches = self._regular_search_with_timeout(query_lower, max_results)

        # Add full image URLs to all products
        matches = self._add_image_urls(matches)

        logger.info(f"Found {len(matches)} products matching '{query}'")
        return matches

    def _fast_search(self, query_lower: str, max_results: int) -> List[Dict[str, Any]]:
        """Fast search using pre-built cache"""
        matches = []

        # Exact match first
        if query_lower in self.fast_search_cache:
            matches.extend(self.fast_search_cache[query_lower][:max_results])
            return matches

        # Partial match
        for key, products in self.fast_search_cache.items():
            if query_lower in key:
                matches.extend(products)
                if len(matches) >= max_results:
                    break

        return matches[:max_results]

    def _regular_search_with_timeout(self, query_lower: str, max_results: int) -> List[Dict[str, Any]]:
        """Regular search with timeout (fallback)"""
        try:
            # Try to get first page quickly
            response = self.session.get(f"{self.api_url}?page=1", timeout=10)
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])

                matches = []
                for product in items:
                    product_name = product.get('product_name', '').lower()
                    product_code = product.get('product_code', '') or ''
                    product_code = product_code.lower()

                    if (query_lower == product_name or
                        query_lower == product_code or
                        query_lower in product_name or
                        query_lower in product_code):
                        matches.append(product)
                        if len(matches) >= max_results:
                            break

                return matches
        except Exception as e:
            logger.warning(f"Regular search failed: {str(e)}")

        return []

    def get_batch_products(self, product_names: List[str]) -> List[Dict[str, Any]]:
        """
        Get multiple products by their names (optimized)

        Args:
            product_names: List of product names to search for

        Returns:
            List of found products with transformed data
        """
        logger.info(f"Batch product request for {len(product_names)} products")

        found_products = []
        not_found = []

        for product_name in product_names:
            product = None

            # Try fast search first
            if self.cache_loaded and self.fast_search_cache:
                product = self._fast_batch_search(product_name)
            else:
                # Fallback to regular search
                product = self._regular_batch_search(product_name)

            if product:
                # Transform product data
                transformed = self._transform_product_data(product)
                found_products.append(transformed)
                logger.info(f"Found product: {product_name}")
            else:
                not_found.append(product_name)
                logger.warning(f"Product not found: {product_name}")

        if not_found:
            logger.warning(f"{len(not_found)} products not found: {not_found}")

        logger.info(f"Batch request complete: {len(found_products)} found, {len(not_found)} not found")
        return found_products

    def _fast_batch_search(self, product_name: str) -> Optional[Dict[str, Any]]:
        """Fast batch search using cache"""
        query_lower = product_name.lower()

        # Exact match first
        if query_lower in self.fast_search_cache:
            return self.fast_search_cache[query_lower][0]

        # Partial match
        for key, products in self.fast_search_cache.items():
            if query_lower in key:
                return products[0]

        return None

    def _regular_batch_search(self, product_name: str) -> Optional[Dict[str, Any]]:
        """Regular batch search (fallback)"""
        try:
            # Try first page only for speed
            response = self.session.get(f"{self.api_url}?page=1", timeout=10)
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])

                query_lower = product_name.lower()
                for product in items:
                    p_name = product.get('product_name', '').lower()
                    p_code = product.get('product_code', '') or ''
                    p_code = p_code.lower()

                    if (p_name == query_lower or p_code == query_lower or
                        query_lower in p_name or query_lower in p_code):
                        return product
        except Exception as e:
            logger.warning(f"Regular batch search failed: {str(e)}")

        return None

    def _transform_product_data(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw product data into standardized format

        Args:
            product: Raw product data from API

        Returns:
            Transformed product data
        """
        # Extract manufacturing sites
        manufacturing_sites = []
        for site in product.get('manufacturing_locations', []):
            manufacturing_sites.append({
                'name': site.get('location_name', 'Unknown'),
                'address': site.get('location_name', ''),
                'location': site.get('location_name', ''),
                'country': self.extract_country(
                    site.get('location_name', ''),
                    site.get('location_state', '')
                ),
                'component': site.get('component', ''),
                'percentage': site.get('component_percentage', 0),
                'coordinates': {
                    'lat': site.get('location_lat'),
                    'lng': site.get('location_lon')
                }
            })

        # Extract raw material sources
        raw_material_sources = []
        for source in product.get('material_locations', []):
            raw_material_sources.append({
                'name': source.get('location_name', 'Unknown'),
                'address': source.get('location_name', ''),
                'location': source.get('location_name', ''),
                'country': self.extract_country(
                    source.get('location_name', ''),
                    source.get('location_state', '')
                ),
                'material': source.get('material', 'Unknown'),
                'percentage': source.get('product_percentage', 0),
                'coordinates': {
                    'lat': source.get('location_lat'),
                    'lng': source.get('location_lon')
                }
            })

        # Extract suppliers/storage locations
        suppliers = []
        for supplier in product.get('storage_locations', []):
            suppliers.append({
                'name': supplier.get('location_name', 'Unknown'),
                'address': supplier.get('location_name', ''),
                'location': supplier.get('location_name', ''),
                'country': self.extract_country(
                    supplier.get('location_name', ''),
                    supplier.get('location_state', '')
                ),
                'type': 'Storage/Distribution',
                'coordinates': {
                    'lat': supplier.get('location_lat'),
                    'lng': supplier.get('location_lon')
                }
            })

        # Product images are not publicly accessible from Product Aware API
        # Frontend will use elegant placeholders

        return {
            'id': product.get('id'),
            'name': product.get('product_name', 'Unknown'),
            'code': product.get('product_code', ''),
            'manufacturer': product.get('manufacturer_name', 'Unknown'),
            'description': product.get('product_description', ''),
            'image_url': '',  # Not accessible
            'thumbnail_url': '',  # Not accessible
            'manufacturingSites': manufacturing_sites,
            'rawMaterialSources': raw_material_sources,
            'suppliers': suppliers,
            'raw': product  # Keep original data for reference
        }

    def extract_locations(self, product: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract all locations from a product for risk assessment

        Args:
            product: Product data (transformed or raw)

        Returns:
            List of location objects
        """
        locations = []

        # Manufacturing locations
        for site in product.get('manufacturingSites', []):
            locations.append({
                'type': 'manufacturing',
                'name': site.get('name', 'Unknown'),
                'address': site.get('address', ''),
                'country': site.get('country', 'Unknown'),
                'component': site.get('component', ''),
                'coordinates': site.get('coordinates', {}),
                'raw': site
            })

        # Raw material sources
        for source in product.get('rawMaterialSources', []):
            locations.append({
                'type': 'raw_material',
                'name': source.get('name', 'Unknown'),
                'address': source.get('address', ''),
                'country': source.get('country', 'Unknown'),
                'material': source.get('material', 'Unknown'),
                'coordinates': source.get('coordinates', {}),
                'raw': source
            })

        # Suppliers/storage
        for supplier in product.get('suppliers', []):
            locations.append({
                'type': 'supplier',
                'name': supplier.get('name', 'Unknown'),
                'address': supplier.get('address', ''),
                'country': supplier.get('country', 'Unknown'),
                'coordinates': supplier.get('coordinates', {}),
                'raw': supplier
            })

        return locations

    def calculate_data_quality(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate data quality metrics for a list of products

        Args:
            products: List of product data

        Returns:
            Data quality metrics
        """
        if not products:
            return {
                'total_products': 0,
                'completeness_score': 0,
                'location_coverage': {},
                'data_quality_issues': []
            }

        total_products = len(products)
        location_counts = {
            'manufacturing': 0,
            'materials': 0,
            'storage': 0,
            'complete': 0
        }

        issues = []

        for product in products:
            has_manufacturing = len(product.get('manufacturingSites', [])) > 0
            has_materials = len(product.get('rawMaterialSources', [])) > 0
            has_storage = len(product.get('suppliers', [])) > 0

            if has_manufacturing:
                location_counts['manufacturing'] += 1
            if has_materials:
                location_counts['materials'] += 1
            if has_storage:
                location_counts['storage'] += 1
            if has_manufacturing and has_materials and has_storage:
                location_counts['complete'] += 1

            # Check for data quality issues
            if not has_manufacturing and not has_materials and not has_storage:
                issues.append(f"Product '{product.get('name', 'Unknown')}' has no location data")

        # Calculate completeness score
        completeness_score = (location_counts['complete'] / total_products) * 100

        return {
            'total_products': total_products,
            'completeness_score': round(completeness_score, 2),
            'location_coverage': {
                'manufacturing': round((location_counts['manufacturing'] / total_products) * 100, 2),
                'materials': round((location_counts['materials'] / total_products) * 100, 2),
                'storage': round((location_counts['storage'] / total_products) * 100, 2),
                'complete': round((location_counts['complete'] / total_products) * 100, 2)
            },
            'data_quality_issues': issues[:10]  # Limit to first 10 issues
        }
