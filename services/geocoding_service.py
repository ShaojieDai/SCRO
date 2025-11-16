"""
Geocoding Service - Python Flask Version
Handles geocoding of addresses using Google Maps API and OpenStreetMap as fallback.
"""

import requests
import logging
import time
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class GeocodingService:
    """Service for geocoding addresses to coordinates"""

    def __init__(self, google_maps_api_key: Optional[str] = None):
        """
        Initialize the geocoding service

        Args:
            google_maps_api_key: Google Maps API key (optional)
        """
        self.google_maps_api_key = google_maps_api_key
        self.google_maps_url = "https://maps.googleapis.com/maps/api/geocode/json"
        self.nominatim_url = "https://nominatim.openstreetmap.org/search"

        # Cache for geocoding results
        self.cache = {}
        self.cache_expiry = {}
        self.cache_duration = timedelta(days=7)  # Geocoding results don't change often

        # Rate limiting for Nominatim (1 request per second)
        self.last_nominatim_request = 0
        self.nominatim_delay = 1.0

        logger.info(f"Geocoding Service initialized")
        if google_maps_api_key:
            logger.info("Google Maps API key provided")
        else:
            logger.info("No Google Maps API key, will use OpenStreetMap Nominatim")

    def _is_cache_valid(self, key: str) -> bool:
        """Check if cached data is still valid"""
        if key not in self.cache or key not in self.cache_expiry:
            return False
        return datetime.now() < self.cache_expiry[key]

    def _set_cache(self, key: str, data: Any) -> None:
        """Store data in cache with expiry"""
        self.cache[key] = data
        self.cache_expiry[key] = datetime.now() + self.cache_duration
        logger.debug(f"💾 Cached geocoding result for: {key}")

    def _get_cache(self, key: str) -> Optional[Any]:
        """Retrieve data from cache if valid"""
        if self._is_cache_valid(key):
            logger.debug(f"✅ Geocoding cache HIT for: {key}")
            return self.cache[key]
        logger.debug(f"❌ Geocoding cache MISS for: {key}")
        return None

    def _geocode_google_maps(self, address: str) -> Optional[Dict[str, Any]]:
        """
        Geocode address using Google Maps API

        Args:
            address: Address to geocode

        Returns:
            Geocoding result or None if failed
        """
        if not self.google_maps_api_key:
            return None

        try:
            params = {
                'address': address,
                'key': self.google_maps_api_key
            }

            logger.debug(f"🌐 Google Maps geocoding: {address}")
            response = requests.get(self.google_maps_url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if data.get('status') == 'OK' and data.get('results'):
                result = data['results'][0]
                geometry = result.get('geometry', {})
                location = geometry.get('location', {})

                return {
                    'lat': location.get('lat'),
                    'lng': location.get('lng'),
                    'formatted_address': result.get('formatted_address', address),
                    'place_id': result.get('place_id'),
                    'provider': 'google_maps'
                }
            else:
                logger.warning(f"⚠️ Google Maps geocoding failed: {data.get('status')}")
                return None

        except Exception as e:
            logger.warning(f"⚠️ Google Maps geocoding error: {str(e)}")
            return None

    def _geocode_nominatim(self, address: str) -> Optional[Dict[str, Any]]:
        """
        Geocode address using OpenStreetMap Nominatim

        Args:
            address: Address to geocode

        Returns:
            Geocoding result or None if failed
        """
        try:
            # Rate limiting for Nominatim
            current_time = time.time()
            time_since_last = current_time - self.last_nominatim_request
            if time_since_last < self.nominatim_delay:
                time.sleep(self.nominatim_delay - time_since_last)

            params = {
                'q': address,
                'format': 'json',
                'limit': 1,
                'addressdetails': 1
            }

            headers = {
                'User-Agent': 'Supply-Chain-Risk-Analysis/1.0'
            }

            logger.debug(f"🌐 Nominatim geocoding: {address}")
            response = requests.get(
                self.nominatim_url,
                params=params,
                headers=headers,
                timeout=10
            )
            response.raise_for_status()

            data = response.json()
            self.last_nominatim_request = time.time()

            if data and len(data) > 0:
                result = data[0]

                return {
                    'lat': float(result.get('lat', 0)),
                    'lng': float(result.get('lon', 0)),
                    'formatted_address': result.get('display_name', address),
                    'place_id': result.get('place_id'),
                    'provider': 'nominatim'
                }
            else:
                logger.warning(f"⚠️ Nominatim geocoding failed: No results")
                return None

        except Exception as e:
            logger.warning(f"⚠️ Nominatim geocoding error: {str(e)}")
            return None

    def geocode_location(self, location: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Geocode a location object

        Args:
            location: Location object with address information

        Returns:
            Location object with added coordinates, or None if geocoding failed
        """
        address = location.get('address', '')
        if not address:
            logger.warning("⚠️ No address provided for geocoding")
            return None

        # Check cache first
        cache_key = f"geocode_{address}"
        cached_result = self._get_cache(cache_key)
        if cached_result:
            return {**location, **cached_result}

        logger.info(f"🗺️ Geocoding address: {address}")

        # Try Google Maps first if API key is available
        geocoded = self._geocode_google_maps(address)

        # Fallback to Nominatim if Google Maps fails
        if not geocoded:
            geocoded = self._geocode_nominatim(address)

        if geocoded:
            # Cache the result
            self._set_cache(cache_key, geocoded)

            # Add coordinates to location
            result = location.copy()
            result.update({
                'lat': geocoded['lat'],
                'lng': geocoded['lng'],
                'formatted_address': geocoded['formatted_address'],
                'geocoded': True,
                'geocoding_provider': geocoded['provider']
            })

            logger.info(f"✅ Geocoded successfully: {address} -> ({geocoded['lat']}, {geocoded['lng']})")
            return result
        else:
            logger.warning(f"❌ Failed to geocode: {address}")
            return None

    def batch_geocode(self, locations: list) -> list:
        """
        Geocode multiple locations

        Args:
            locations: List of location objects

        Returns:
            List of geocoded location objects
        """
        logger.info(f"🗺️ Batch geocoding {len(locations)} locations")

        geocoded_locations = []
        successful = 0
        failed = 0

        for location in locations:
            try:
                geocoded = self.geocode_location(location)
                if geocoded:
                    geocoded_locations.append(geocoded)
                    successful += 1
                else:
                    # Add original location without coordinates
                    geocoded_locations.append(location)
                    failed += 1
            except Exception as e:
                logger.warning(f"⚠️ Error geocoding location {location.get('address', 'Unknown')}: {str(e)}")
                geocoded_locations.append(location)
                failed += 1

        logger.info(f"🎉 Batch geocoding complete: {successful} successful, {failed} failed")
        return geocoded_locations

    def reverse_geocode(self, lat: float, lng: float) -> Optional[Dict[str, Any]]:
        """
        Reverse geocode coordinates to address

        Args:
            lat: Latitude
            lng: Longitude

        Returns:
            Address information or None if failed
        """
        cache_key = f"reverse_{lat}_{lng}"
        cached_result = self._get_cache(cache_key)
        if cached_result:
            return cached_result

        try:
            # Try Google Maps first
            if self.google_maps_api_key:
                params = {
                    'latlng': f"{lat},{lng}",
                    'key': self.google_maps_api_key
                }

                response = requests.get(self.google_maps_url, params=params, timeout=10)
                response.raise_for_status()

                data = response.json()
                if data.get('status') == 'OK' and data.get('results'):
                    result = data['results'][0]
                    reverse_result = {
                        'formatted_address': result.get('formatted_address'),
                        'place_id': result.get('place_id'),
                        'provider': 'google_maps'
                    }
                    self._set_cache(cache_key, reverse_result)
                    return reverse_result

            # Fallback to Nominatim
            params = {
                'lat': lat,
                'lon': lng,
                'format': 'json',
                'addressdetails': 1
            }

            headers = {
                'User-Agent': 'Supply-Chain-Risk-Analysis/1.0'
            }

            response = requests.get(
                self.nominatim_url,
                params=params,
                headers=headers,
                timeout=10
            )
            response.raise_for_status()

            data = response.json()
            if data and len(data) > 0:
                result = data[0]
                reverse_result = {
                    'formatted_address': result.get('display_name'),
                    'place_id': result.get('place_id'),
                    'provider': 'nominatim'
                }
                self._set_cache(cache_key, reverse_result)
                return reverse_result

            return None

        except Exception as e:
            logger.warning(f"⚠️ Reverse geocoding error: {str(e)}")
            return None
