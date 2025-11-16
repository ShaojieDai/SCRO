"""
Risk Assessment Service - Python Flask Version
Handles supply chain risk assessment calculations including HHI and geographic risk.
"""

import logging
import math
from typing import List, Dict, Any, Tuple
from collections import defaultdict, Counter

logger = logging.getLogger(__name__)

class RiskAssessmentService:
    """Service for assessing supply chain risks"""

    def __init__(self):
        """Initialize the risk assessment service"""
        self.climate_risk_data = self._load_climate_risk_data()
        self.geopolitical_risk_data = self._load_geopolitical_risk_data()
        logger.info("Risk Assessment Service initialized")

    def _load_climate_risk_data(self) -> Dict[str, float]:
        """Load climate risk data for different countries/regions"""
        # This is a simplified climate risk dataset
        # In a real implementation, this would come from a database or external API
        return {
            'Australia': 0.3,  # Low to moderate climate risk
            'China': 0.6,      # Moderate to high climate risk
            'USA': 0.4,        # Moderate climate risk
            'Germany': 0.2,    # Low climate risk
            'Japan': 0.5,      # Moderate climate risk
            'India': 0.7,      # High climate risk
            'Brazil': 0.6,     # Moderate to high climate risk
            'Canada': 0.3,     # Low to moderate climate risk
            'United Kingdom': 0.3,  # Low to moderate climate risk
            'France': 0.2,     # Low climate risk
            'Italy': 0.4,      # Moderate climate risk
            'Spain': 0.5,      # Moderate climate risk
            'Netherlands': 0.3, # Low to moderate climate risk
            'South Korea': 0.4, # Moderate climate risk
            'Taiwan': 0.5,     # Moderate climate risk
            'Thailand': 0.6,   # Moderate to high climate risk
            'Vietnam': 0.6,    # Moderate to high climate risk
            'Indonesia': 0.7,  # High climate risk
            'Malaysia': 0.6,   # Moderate to high climate risk
            'Philippines': 0.7, # High climate risk
            'Mexico': 0.5,     # Moderate climate risk
            'Turkey': 0.5,     # Moderate climate risk
            'Poland': 0.3,     # Low to moderate climate risk
            'Czech Republic': 0.3, # Low to moderate climate risk
            'Hungary': 0.4,    # Moderate climate risk
            'Romania': 0.4,    # Moderate climate risk
            'Bulgaria': 0.4,   # Moderate climate risk
            'Croatia': 0.4,    # Moderate climate risk
            'Slovakia': 0.3,   # Low to moderate climate risk
            'Slovenia': 0.3,   # Low to moderate climate risk
            'Estonia': 0.3,    # Low to moderate climate risk
            'Latvia': 0.3,     # Low to moderate climate risk
            'Lithuania': 0.3,  # Low to moderate climate risk
            'Finland': 0.2,    # Low climate risk
            'Sweden': 0.2,     # Low climate risk
            'Norway': 0.2,     # Low climate risk
            'Denmark': 0.3,    # Low to moderate climate risk
            'Switzerland': 0.2, # Low climate risk
            'Austria': 0.2,    # Low climate risk
            'Belgium': 0.3,    # Low to moderate climate risk
            'Luxembourg': 0.3, # Low to moderate climate risk
            'Ireland': 0.3,    # Low to moderate climate risk
            'Portugal': 0.4,   # Moderate climate risk
            'Greece': 0.5,     # Moderate climate risk
            'Cyprus': 0.5,     # Moderate climate risk
            'Malta': 0.4,      # Moderate climate risk
            'Iceland': 0.2,    # Low climate risk
            'New Zealand': 0.3, # Low to moderate climate risk
            'South Africa': 0.6, # Moderate to high climate risk
            'Egypt': 0.7,      # High climate risk
            'Morocco': 0.6,    # Moderate to high climate risk
            'Tunisia': 0.6,    # Moderate to high climate risk
            'Algeria': 0.6,    # Moderate to high climate risk
            'Libya': 0.7,      # High climate risk
            'Sudan': 0.8,      # Very high climate risk
            'Ethiopia': 0.7,   # High climate risk
            'Kenya': 0.6,      # Moderate to high climate risk
            'Nigeria': 0.7,    # High climate risk
            'Ghana': 0.6,      # Moderate to high climate risk
            'Senegal': 0.6,    # Moderate to high climate risk
            'Ivory Coast': 0.6, # Moderate to high climate risk
            'Cameroon': 0.6,   # Moderate to high climate risk
            'Angola': 0.6,     # Moderate to high climate risk
            'Mozambique': 0.6, # Moderate to high climate risk
            'Tanzania': 0.6,   # Moderate to high climate risk
            'Uganda': 0.6,     # Moderate to high climate risk
            'Rwanda': 0.6,     # Moderate to high climate risk
            'Burundi': 0.6,    # Moderate to high climate risk
            'Madagascar': 0.6, # Moderate to high climate risk
            'Mauritius': 0.5,  # Moderate climate risk
            'Seychelles': 0.5, # Moderate climate risk
            'Reunion': 0.5,    # Moderate climate risk
            'Mayotte': 0.5,    # Moderate climate risk
            'Comoros': 0.6,    # Moderate to high climate risk
            'Djibouti': 0.7,   # High climate risk
            'Somalia': 0.8,    # Very high climate risk
            'Eritrea': 0.7,    # High climate risk
            'Chad': 0.7,       # High climate risk
            'Niger': 0.7,      # High climate risk
            'Mali': 0.7,       # High climate risk
            'Burkina Faso': 0.7, # High climate risk
            'Guinea': 0.6,     # Moderate to high climate risk
            'Sierra Leone': 0.6, # Moderate to high climate risk
            'Liberia': 0.6,    # Moderate to high climate risk
            'Gambia': 0.6,     # Moderate to high climate risk
            'Guinea-Bissau': 0.6, # Moderate to high climate risk
            'Cape Verde': 0.5, # Moderate climate risk
            'Sao Tome and Principe': 0.5, # Moderate climate risk
            'Equatorial Guinea': 0.6, # Moderate to high climate risk
            'Gabon': 0.6,      # Moderate to high climate risk
            'Republic of the Congo': 0.6, # Moderate to high climate risk
            'Democratic Republic of the Congo': 0.7, # High climate risk
            'Central African Republic': 0.7, # High climate risk
            'Zambia': 0.6,     # Moderate to high climate risk
            'Zimbabwe': 0.6,   # Moderate to high climate risk
            'Botswana': 0.6,   # Moderate to high climate risk
            'Namibia': 0.6,    # Moderate to high climate risk
            'Lesotho': 0.6,    # Moderate to high climate risk
            'Swaziland': 0.6,  # Moderate to high climate risk
            'Malawi': 0.6,     # Moderate to high climate risk
            'Zambia': 0.6,     # Moderate to high climate risk
            'Zimbabwe': 0.6,   # Moderate to high climate risk
            'Botswana': 0.6,   # Moderate to high climate risk
            'Namibia': 0.6,    # Moderate to high climate risk
            'Lesotho': 0.6,    # Moderate to high climate risk
            'Swaziland': 0.6,  # Moderate to high climate risk
            'Malawi': 0.6,     # Moderate to high climate risk
            'Unknown': 0.5     # Default moderate risk for unknown countries
        }

    def _load_geopolitical_risk_data(self) -> Dict[str, float]:
        """Load geopolitical risk data for different countries/regions"""
        # This is a simplified geopolitical risk dataset
        # In a real implementation, this would come from a database or external API
        return {
            'Australia': 0.1,  # Very low geopolitical risk
            'China': 0.4,      # Moderate geopolitical risk
            'USA': 0.2,        # Low geopolitical risk
            'Germany': 0.1,    # Very low geopolitical risk
            'Japan': 0.2,      # Low geopolitical risk
            'India': 0.3,      # Low to moderate geopolitical risk
            'Brazil': 0.3,     # Low to moderate geopolitical risk
            'Canada': 0.1,     # Very low geopolitical risk
            'United Kingdom': 0.2, # Low geopolitical risk
            'France': 0.2,     # Low geopolitical risk
            'Italy': 0.2,      # Low geopolitical risk
            'Spain': 0.2,      # Low geopolitical risk
            'Netherlands': 0.1, # Very low geopolitical risk
            'South Korea': 0.3, # Low to moderate geopolitical risk
            'Taiwan': 0.5,     # Moderate geopolitical risk
            'Thailand': 0.3,   # Low to moderate geopolitical risk
            'Vietnam': 0.3,    # Low to moderate geopolitical risk
            'Indonesia': 0.3,  # Low to moderate geopolitical risk
            'Malaysia': 0.3,   # Low to moderate geopolitical risk
            'Philippines': 0.4, # Moderate geopolitical risk
            'Mexico': 0.4,     # Moderate geopolitical risk
            'Turkey': 0.5,     # Moderate geopolitical risk
            'Poland': 0.2,     # Low geopolitical risk
            'Czech Republic': 0.2, # Low geopolitical risk
            'Hungary': 0.3,    # Low to moderate geopolitical risk
            'Romania': 0.3,    # Low to moderate geopolitical risk
            'Bulgaria': 0.3,   # Low to moderate geopolitical risk
            'Croatia': 0.2,    # Low geopolitical risk
            'Slovakia': 0.2,   # Low geopolitical risk
            'Slovenia': 0.2,   # Low geopolitical risk
            'Estonia': 0.3,    # Low to moderate geopolitical risk
            'Latvia': 0.3,     # Low to moderate geopolitical risk
            'Lithuania': 0.3,  # Low to moderate geopolitical risk
            'Finland': 0.2,    # Low geopolitical risk
            'Sweden': 0.1,     # Very low geopolitical risk
            'Norway': 0.1,     # Very low geopolitical risk
            'Denmark': 0.1,    # Very low geopolitical risk
            'Switzerland': 0.1, # Very low geopolitical risk
            'Austria': 0.1,    # Very low geopolitical risk
            'Belgium': 0.1,    # Very low geopolitical risk
            'Luxembourg': 0.1, # Very low geopolitical risk
            'Ireland': 0.1,    # Very low geopolitical risk
            'Portugal': 0.2,   # Low geopolitical risk
            'Greece': 0.3,     # Low to moderate geopolitical risk
            'Cyprus': 0.4,     # Moderate geopolitical risk
            'Malta': 0.2,      # Low geopolitical risk
            'Iceland': 0.1,    # Very low geopolitical risk
            'New Zealand': 0.1, # Very low geopolitical risk
            'South Africa': 0.4, # Moderate geopolitical risk
            'Egypt': 0.6,      # High geopolitical risk
            'Morocco': 0.4,    # Moderate geopolitical risk
            'Tunisia': 0.4,    # Moderate geopolitical risk
            'Algeria': 0.5,    # Moderate geopolitical risk
            'Libya': 0.7,      # High geopolitical risk
            'Sudan': 0.8,      # Very high geopolitical risk
            'Ethiopia': 0.5,   # Moderate geopolitical risk
            'Kenya': 0.4,      # Moderate geopolitical risk
            'Nigeria': 0.5,    # Moderate geopolitical risk
            'Ghana': 0.3,      # Low to moderate geopolitical risk
            'Senegal': 0.3,    # Low to moderate geopolitical risk
            'Ivory Coast': 0.4, # Moderate geopolitical risk
            'Cameroon': 0.4,   # Moderate geopolitical risk
            'Angola': 0.4,     # Moderate geopolitical risk
            'Mozambique': 0.4, # Moderate geopolitical risk
            'Tanzania': 0.3,   # Low to moderate geopolitical risk
            'Uganda': 0.4,     # Moderate geopolitical risk
            'Rwanda': 0.3,     # Low to moderate geopolitical risk
            'Burundi': 0.5,    # Moderate geopolitical risk
            'Madagascar': 0.3, # Low to moderate geopolitical risk
            'Mauritius': 0.2,  # Low geopolitical risk
            'Seychelles': 0.2, # Low geopolitical risk
            'Reunion': 0.2,    # Low geopolitical risk
            'Mayotte': 0.2,    # Low geopolitical risk
            'Comoros': 0.3,    # Low to moderate geopolitical risk
            'Djibouti': 0.5,   # Moderate geopolitical risk
            'Somalia': 0.8,    # Very high geopolitical risk
            'Eritrea': 0.6,    # High geopolitical risk
            'Chad': 0.6,       # High geopolitical risk
            'Niger': 0.6,      # High geopolitical risk
            'Mali': 0.6,       # High geopolitical risk
            'Burkina Faso': 0.6, # High geopolitical risk
            'Guinea': 0.4,     # Moderate geopolitical risk
            'Sierra Leone': 0.4, # Moderate geopolitical risk
            'Liberia': 0.4,    # Moderate geopolitical risk
            'Gambia': 0.3,     # Low to moderate geopolitical risk
            'Guinea-Bissau': 0.4, # Moderate geopolitical risk
            'Cape Verde': 0.2, # Low geopolitical risk
            'Sao Tome and Principe': 0.2, # Low geopolitical risk
            'Equatorial Guinea': 0.4, # Moderate geopolitical risk
            'Gabon': 0.3,      # Low to moderate geopolitical risk
            'Republic of the Congo': 0.4, # Moderate geopolitical risk
            'Democratic Republic of the Congo': 0.6, # High geopolitical risk
            'Central African Republic': 0.7, # High geopolitical risk
            'Zambia': 0.3,     # Low to moderate geopolitical risk
            'Zimbabwe': 0.5,   # Moderate geopolitical risk
            'Botswana': 0.2,   # Low geopolitical risk
            'Namibia': 0.3,    # Low to moderate geopolitical risk
            'Lesotho': 0.3,    # Low to moderate geopolitical risk
            'Swaziland': 0.3,  # Low to moderate geopolitical risk
            'Malawi': 0.3,     # Low to moderate geopolitical risk
            'Unknown': 0.5     # Default moderate risk for unknown countries
        }

    def calculate_hhi(self, locations: List[Dict[str, Any]], location_type: str = None) -> float:
        """
        Calculate Herfindahl-Hirschman Index for geographic concentration

        Args:
            locations: List of location objects
            location_type: Optional filter for specific location type

        Returns:
            HHI value (0-1, where 1 is perfect concentration)
        """
        if not locations:
            return 0.0

        # Filter by location type if specified
        if location_type:
            filtered_locations = [loc for loc in locations if loc.get('type') == location_type]
        else:
            filtered_locations = locations

        if not filtered_locations:
            return 0.0

        # Count locations by country
        country_counts = Counter()
        for location in filtered_locations:
            country = location.get('country', 'Unknown')
            country_counts[country] += 1

        total_locations = len(filtered_locations)
        if total_locations == 0:
            return 0.0

        # Calculate HHI
        hhi = 0.0
        for count in country_counts.values():
            market_share = count / total_locations
            hhi += market_share ** 2

        return hhi

    def assess_geographic_risk(self, locations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Assess geographic concentration and risk

        Args:
            locations: List of location objects

        Returns:
            Geographic risk assessment
        """
        if not locations:
            return {
                'total_locations': 0,
                'countries': 0,
                'hhi': 0.0,
                'concentration_risk': 'low',
                'country_distribution': {},
                'risk_factors': []
            }

        # Calculate overall HHI
        overall_hhi = self.calculate_hhi(locations)

        # Calculate HHI by location type
        manufacturing_hhi = self.calculate_hhi(locations, 'manufacturing')
        materials_hhi = self.calculate_hhi(locations, 'raw_material')
        storage_hhi = self.calculate_hhi(locations, 'supplier')

        # --- Segmented supply-path risk (NEW) ---
        # Part 1: Raw materials → Manufacturing
        material_countries = [loc.get('country', 'Unknown') for loc in locations if loc.get('type') == 'raw_material']
        manufacturing_countries = [loc.get('country', 'Unknown') for loc in locations if loc.get('type') == 'manufacturing']
        storage_countries = [loc.get('country', 'Unknown') for loc in locations if loc.get('type') == 'supplier']

        # Overlap ratios (co-location lowers risk)
        def overlap_ratio(source_countries: List[str], target_countries: List[str]) -> float:
            if not source_countries:
                return 0.0
            target_set = set(c for c in target_countries if c)
            overlap = sum(1 for c in source_countries if c in target_set)
            return overlap / max(len(source_countries), 1)

        overlap_m2m = overlap_ratio(material_countries, manufacturing_countries)
        overlap_manu_to_store = overlap_ratio(manufacturing_countries, storage_countries)

        # Base segment HHI = average of both ends' HHIs
        base_seg_m2m = (materials_hhi + manufacturing_hhi) / 2.0
        base_seg_manu_to_store = (manufacturing_hhi + storage_hhi) / 2.0

        # Apply co-location reduction factor: more overlap -> lower effective risk
        # reduction_factor k controls how much to reward same-country sourcing (0.0-0.8 typical)
        k = 0.5
        adj_seg_m2m = max(base_seg_m2m * (1.0 - k * overlap_m2m), 0.0)
        adj_seg_manu_to_store = max(base_seg_manu_to_store * (1.0 - k * overlap_manu_to_store), 0.0)

        # Count countries
        countries = set(loc.get('country', 'Unknown') for loc in locations)
        country_distribution = Counter(loc.get('country', 'Unknown') for loc in locations)

        # Determine concentration risk level
        if overall_hhi >= 0.7:
            concentration_risk = 'very_high'
        elif overall_hhi >= 0.5:
            concentration_risk = 'high'
        elif overall_hhi >= 0.3:
            concentration_risk = 'moderate'
        else:
            concentration_risk = 'low'

        # Identify risk factors
        risk_factors = []

        if overall_hhi >= 0.5:
            risk_factors.append(f"High geographic concentration (HHI: {overall_hhi:.3f})")

        if len(countries) <= 2:
            risk_factors.append(f"Limited geographic diversity ({len(countries)} countries)")

        # Check for high-risk countries
        high_risk_countries = []
        for country in countries:
            climate_risk = self.climate_risk_data.get(country, 0.5)
            geo_risk = self.geopolitical_risk_data.get(country, 0.5)

            if climate_risk >= 0.7 or geo_risk >= 0.7:
                high_risk_countries.append({
                    'country': country,
                    'climate_risk': climate_risk,
                    'geopolitical_risk': geo_risk
                })

        if high_risk_countries:
            risk_factors.append(f"High-risk countries: {[c['country'] for c in high_risk_countries]}")

        return {
            'total_locations': len(locations),
            'countries': len(countries),
            'hhi': round(overall_hhi, 3),
            'concentration_risk': concentration_risk,
            'country_distribution': dict(country_distribution),
            'hhi_by_type': {
                'manufacturing': round(manufacturing_hhi, 3),
                'materials': round(materials_hhi, 3),
                'storage': round(storage_hhi, 3)
            },
            'hhi_by_segment': {
                'materials_to_manufacturing': {
                    'base_hhi': round(base_seg_m2m, 3),
                    'adjusted_hhi': round(adj_seg_m2m, 3),
                    'overlap_ratio': round(overlap_m2m, 3)
                },
                'manufacturing_to_storage': {
                    'base_hhi': round(base_seg_manu_to_store, 3),
                    'adjusted_hhi': round(adj_seg_manu_to_store, 3),
                    'overlap_ratio': round(overlap_manu_to_store, 3)
                }
            },
            'risk_factors': risk_factors,
            'high_risk_countries': high_risk_countries
        }

    def assess_supply_chain_risk(self, locations: List[Dict[str, Any]], products: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Comprehensive supply chain risk assessment

        Args:
            locations: List of geocoded location objects

        Returns:
            Complete risk assessment results
        """
        logger.info(f"⚠️ Assessing supply chain risk for {len(locations)} locations")

        if not locations:
            return {
                'overall_risk': 'unknown',
                'risk_score': 0.0,
                'geographic_risk': {},
                'climate_risk': {},
                'geopolitical_risk': {},
                'concentration_risk': {},
                'recommendations': ['No locations available for risk assessment']
            }

        # Geographic risk assessment
        geographic_risk = self.assess_geographic_risk(locations)

        # Climate risk assessment
        climate_risk = self._assess_climate_risk(locations)

        # Geopolitical risk assessment
        geopolitical_risk = self._assess_geopolitical_risk(locations)

        # Lead time risk assessment (NEW)
        lead_time_risk = self._assess_lead_time_risk(products or [])

        # Calculate overall risk score (70% HHI + 30% Lead Time as requested)
        # Keep legacy components for transparency in the response
        risk_score = self._calculate_overall_risk_score_with_lead_time(
            geographic_risk, lead_time_risk
        )

        # Determine overall risk level
        if risk_score >= 0.8:
            overall_risk = 'very_high'
        elif risk_score >= 0.6:
            overall_risk = 'high'
        elif risk_score >= 0.4:
            overall_risk = 'moderate'
        elif risk_score >= 0.2:
            overall_risk = 'low'
        else:
            overall_risk = 'very_low'

        # Generate recommendations
        recommendations = self._generate_recommendations(
            geographic_risk, climate_risk, geopolitical_risk
        )

        return {
            'overall_risk': overall_risk,
            'risk_score': round(risk_score, 3),
            'geographic_risk': geographic_risk,
            'climate_risk': climate_risk,
            'geopolitical_risk': geopolitical_risk,
            'lead_time_risk': lead_time_risk,
            'concentration_risk': {
                'hhi': geographic_risk['hhi'],
                'concentration_level': geographic_risk['concentration_risk'],
                'countries': geographic_risk['countries'],
                'country_distribution': geographic_risk['country_distribution']
            },
            'recommendations': recommendations,
            'assessment_timestamp': logger.info("Risk assessment completed")
        }

    def _assess_lead_time_risk(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Assess lead time risk from Product Aware 'lead_time' or availability fields.
        Returns average_risk in [0,1] and details per product.
        Rules:
          - "In Stock Australia" -> risk 0.0
          - < 15 working days (~3 weeks) -> low (0.2)
          - ~5 weeks -> medium (0.5-0.6)
          - > 10 weeks -> high (0.9)
          - Unknown -> 0.5
        """
        if not products:
            return {'average_risk': 0.0, 'items': []}

        def parse_weeks(text: str) -> float:
            if not text:
                return -1
            t = text.strip().lower()
            if 'in stock australia' in t or 'in-stock australia' in t:
                return 0  # special flag handled later
            # extract number of weeks/days
            import re
            m = re.search(r'(\d+\.?\d*)\s*weeks?', t)
            if m:
                try:
                    return float(m.group(1))
                except Exception:
                    pass
            m = re.search(r'(\d+)\s*days?', t)
            if m:
                try:
                    days = int(m.group(1))
                    return days / 5.0  # approx working weeks
                except Exception:
                    pass
            return -1

        def risk_from_text(text: str) -> float:
            if not text:
                return 0.5
            t = text.strip().lower()
            if 'in stock australia' in t or 'in-stock australia' in t:
                return 0.0
            weeks = parse_weeks(t)
            if weeks == 0:
                return 0.0
            if weeks < 0:  # unknown
                return 0.5
            if weeks <= 3:  # <15 working days
                return 0.2
            if weeks <= 6:  # ~5 weeks
                return 0.6
            if weeks > 10:
                return 0.9
            # interpolate for 6-10 weeks
            # map 6->0.6, 10->0.85
            return min(0.85, 0.6 + (max(weeks, 6) - 6) * (0.25 / 4))

        items = []
        total = 0.0
        count = 0
        for p in products:
            raw = p.get('raw', {}) if isinstance(p, dict) else {}
            lt = raw.get('lead_time') or raw.get('leadTime') or raw.get('availability') or ''
            score = risk_from_text(str(lt))
            items.append({'product': p.get('name', 'Unknown'), 'lead_time': lt, 'risk': round(score, 3)})
            total += score
            count += 1

        avg = total / count if count else 0.0
        return {'average_risk': round(avg, 3), 'items': items}

    def _calculate_overall_risk_score_with_lead_time(self, geographic_risk: Dict, lead_time_risk: Dict) -> float:
        """Combine HHI (70%) and lead time risk (30%) into a single score in [0,1]."""
        hhi_score = geographic_risk.get('hhi', 0.0)
        lt_score = lead_time_risk.get('average_risk', 0.0)
        return min(max(0.7 * hhi_score + 0.3 * lt_score, 0.0), 1.0)

    def _assess_climate_risk(self, locations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Assess climate risk for locations"""
        if not locations:
            return {'average_risk': 0.0, 'high_risk_locations': []}

        total_risk = 0.0
        high_risk_locations = []

        for location in locations:
            country = location.get('country', 'Unknown')
            climate_risk = self.climate_risk_data.get(country, 0.5)
            total_risk += climate_risk

            if climate_risk >= 0.7:
                high_risk_locations.append({
                    'name': location.get('name', 'Unknown'),
                    'country': country,
                    'risk_level': climate_risk,
                    'type': location.get('type', 'unknown')
                })

        average_risk = total_risk / len(locations)

        return {
            'average_risk': round(average_risk, 3),
            'high_risk_locations': high_risk_locations,
            'risk_level': 'high' if average_risk >= 0.6 else 'moderate' if average_risk >= 0.4 else 'low'
        }

    def _assess_geopolitical_risk(self, locations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Assess geopolitical risk for locations"""
        if not locations:
            return {'average_risk': 0.0, 'high_risk_locations': []}

        total_risk = 0.0
        high_risk_locations = []

        for location in locations:
            country = location.get('country', 'Unknown')
            geo_risk = self.geopolitical_risk_data.get(country, 0.5)
            total_risk += geo_risk

            if geo_risk >= 0.7:
                high_risk_locations.append({
                    'name': location.get('name', 'Unknown'),
                    'country': country,
                    'risk_level': geo_risk,
                    'type': location.get('type', 'unknown')
                })

        average_risk = total_risk / len(locations)

        return {
            'average_risk': round(average_risk, 3),
            'high_risk_locations': high_risk_locations,
            'risk_level': 'high' if average_risk >= 0.6 else 'moderate' if average_risk >= 0.4 else 'low'
        }

    def _calculate_overall_risk_score(self, geographic_risk: Dict, climate_risk: Dict, geopolitical_risk: Dict) -> float:
        """Calculate overall risk score from individual risk components"""
        # Weighted combination of risk factors
        weights = {
            'geographic': 0.4,    # Geographic concentration is most important
            'climate': 0.3,       # Climate risk is significant
            'geopolitical': 0.3   # Geopolitical risk is significant
        }

        # Convert HHI to risk score (higher HHI = higher risk)
        hhi_score = geographic_risk.get('hhi', 0.0)

        # Climate and geopolitical risk scores
        climate_score = climate_risk.get('average_risk', 0.0)
        geopolitical_score = geopolitical_risk.get('average_risk', 0.0)

        # Calculate weighted average
        overall_score = (
            weights['geographic'] * hhi_score +
            weights['climate'] * climate_score +
            weights['geopolitical'] * geopolitical_score
        )

        return min(overall_score, 1.0)  # Cap at 1.0

    def _generate_recommendations(self, geographic_risk: Dict, climate_risk: Dict, geopolitical_risk: Dict) -> List[str]:
        """Generate risk mitigation recommendations"""
        recommendations = []

        # Geographic concentration recommendations
        hhi = geographic_risk.get('hhi', 0.0)
        if hhi >= 0.7:
            recommendations.append("Consider diversifying suppliers across more countries to reduce geographic concentration risk")
        elif hhi >= 0.5:
            recommendations.append("Monitor geographic concentration and consider adding suppliers in different regions")

        # Climate risk recommendations
        if climate_risk.get('risk_level') == 'high':
            recommendations.append("Implement climate risk mitigation strategies for high-risk locations")
            high_risk_countries = [loc['country'] for loc in climate_risk.get('high_risk_locations', [])]
            if high_risk_countries:
                recommendations.append(f"Consider alternative suppliers outside high climate risk countries: {', '.join(set(high_risk_countries))}")

        # Geopolitical risk recommendations
        if geopolitical_risk.get('risk_level') == 'high':
            recommendations.append("Develop contingency plans for geopolitical disruptions in high-risk regions")
            high_risk_countries = [loc['country'] for loc in geopolitical_risk.get('high_risk_locations', [])]
            if high_risk_countries:
                recommendations.append(f"Monitor political stability in: {', '.join(set(high_risk_countries))}")

        # General recommendations
        if not recommendations:
            recommendations.append("Supply chain risk levels are acceptable. Continue monitoring for changes.")

        return recommendations
