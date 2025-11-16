"""
Supply Chain Risk Analysis - Flask Backend
Main application file for the Flask-based backend server.
"""

import os
import json
import logging
import requests
from urllib.parse import quote
from datetime import datetime
from flask import Flask, request, jsonify, render_template, Response
from flask_cors import CORS
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger(__name__)

# Create Flask app with static and templates folders for PythonAnywhere deployment
app = Flask(__name__,
            static_folder='static',
            static_url_path='/static',
            template_folder='templates')
CORS(app)

# Configuration
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['JSON_SORT_KEYS'] = False

# Environment variables
PRODUCT_AWARE_API_URL = os.getenv('PRODUCT_AWARE_API_URL', 'https://productaware.au/api/products')
PRODUCT_AWARE_API_KEY = os.getenv('PRODUCT_AWARE_API_KEY')
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')

# Validate required environment variables
if not PRODUCT_AWARE_API_KEY:
    logger.error("❌ PRODUCT_AWARE_API_KEY not found in environment variables")
    raise ValueError("PRODUCT_AWARE_API_KEY is required")

logger.info("Starting Supply Chain Risk Analysis Flask Backend")
logger.info(f"Environment variables loaded:")
logger.info(f"  PORT: {os.getenv('PORT', '5000')}")
logger.info(f"  PRODUCT_AWARE_API_URL: {PRODUCT_AWARE_API_URL}")
logger.info(f"  PRODUCT_AWARE_API_KEY: {'Bearer ***' if PRODUCT_AWARE_API_KEY else 'NOT SET'}")
logger.info(f"  GOOGLE_MAPS_API_KEY: {'***' if GOOGLE_MAPS_API_KEY else 'NOT SET'}")
logger.info(f"  OPENAI_MODEL: {OPENAI_MODEL if OPENAI_API_KEY else 'DISABLED (missing key)'}")
logger.info("")

# Import services
from services.product_aware_service import ProductAwareService
from services.geocoding_service import GeocodingService
from services.risk_assessment_service import RiskAssessmentService

# Initialize services
product_aware_service = ProductAwareService(PRODUCT_AWARE_API_URL, PRODUCT_AWARE_API_KEY)
geocoding_service = GeocodingService(GOOGLE_MAPS_API_KEY)
risk_assessment_service = RiskAssessmentService()

openai_client = None
if OPENAI_API_KEY:
    try:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        logger.info("OpenAI client initialized for chat assistant")
    except Exception as exc:
        logger.error(f"Failed to initialize OpenAI client: {exc}")
else:
    logger.warning("OPENAI_API_KEY not set; chat assistant endpoint will be disabled")

# Health check endpoint
@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0',
        'service': 'Supply Chain Risk Analysis Flask Backend'
    })

# Cache status endpoint
@app.route('/api/cache/status', methods=['GET'])
def cache_status():
    """Check cache loading status"""
    return jsonify({
        'cache_loaded': product_aware_service.cache_loaded,
        'cache_loading': product_aware_service.cache_loading,
        'cache_size': len(product_aware_service.fast_search_cache),
        'status': 'ready' if product_aware_service.cache_loaded else 'loading'
    })

@app.route('/api/debug/check-images', methods=['GET'])
def debug_check_images():
    """Debug endpoint to check if products have image field in raw cache"""
    try:
        if not product_aware_service.cache_loaded:
            return jsonify({'error': 'Cache not loaded yet'}), 503

        # Sample products from cache
        samples = []
        for key, products_list in product_aware_service.fast_search_cache.items():
            samples.extend(products_list[:2])
            if len(samples) >= 10:
                break

        # Check for image field
        with_image = 0
        without_image = 0
        sample_details = []

        for product in samples[:10]:
            has_image = 'image' in product and product.get('image')
            if has_image:
                with_image += 1
            else:
                without_image += 1

            sample_details.append({
                'id': product.get('id'),
                'name': product.get('product_name', 'Unknown'),
                'has_image_field': 'image' in product,
                'image_value': product.get('image', None),
                'has_image_value': bool(product.get('image'))
            })

        # Count all
        total = 0
        total_with_image = 0
        for key, products_list in product_aware_service.fast_search_cache.items():
            for product in products_list:
                total += 1
                if product.get('image'):
                    total_with_image += 1

        # Get first product complete structure
        first_product = samples[0] if samples else {}

        return jsonify({
            'success': True,
            'cache_size': total,
            'sample_count': len(samples),
            'with_image': with_image,
            'without_image': without_image,
            'total_with_image': total_with_image,
            'total_without_image': total - total_with_image,
            'percentage_with_images': (total_with_image / total * 100) if total > 0 else 0,
            'sample_details': sample_details,
            'first_product_fields': list(first_product.keys()) if first_product else [],
            'first_product_sample': first_product
        })
    except Exception as e:
        logger.error(f"Error checking images: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/proxy/image', methods=['GET'])
def proxy_image():
    """Proxy product images from Product Aware API with authentication"""
    try:
        image_path = request.args.get('path', '')
        if not image_path:
            return jsonify({'error': 'No image path provided'}), 400

        # Construct the full image URL - try different possible endpoints
        possible_urls = [
            f"https://productaware.au/media/{image_path}",
            f"https://productaware.au/{image_path}",
            f"https://productaware.au/api/media/{image_path}"
        ]

        # Try each URL with authentication
        headers = {
            'Authorization': PRODUCT_AWARE_API_KEY if PRODUCT_AWARE_API_KEY.startswith('Bearer ') else f'Bearer {PRODUCT_AWARE_API_KEY}'
        }

        for url in possible_urls:
            try:
                response = requests.get(url, headers=headers, timeout=10, stream=True)
                if response.status_code == 200:
                    # Successfully got the image, proxy it to frontend
                    return Response(
                        response.content,
                        content_type=response.headers.get('Content-Type', 'image/jpeg'),
                        headers={
                            'Cache-Control': 'public, max-age=86400',  # Cache for 24 hours
                            'Access-Control-Allow-Origin': '*'
                        }
                    )
            except Exception as e:
                logger.warning(f"Failed to fetch from {url}: {str(e)}")
                continue

        # If all URLs fail, return 404
        logger.error(f"Could not fetch image from any URL for path: {image_path}")
        return jsonify({'error': 'Image not found'}), 404

    except Exception as e:
        logger.error(f"Error in image proxy: {str(e)}")
        return jsonify({'error': f'Failed to fetch image: {str(e)}'}), 500

# Product routes
@app.route('/api/products/batch', methods=['POST'])
def get_batch_products():
    """Get batch product data by product names"""
    try:
        data = request.get_json()
        product_names = data.get('productNames', [])

        if not product_names:
            return jsonify({'error': 'No product names provided'}), 400

        logger.info(f"🔍 Batch product request for {len(product_names)} products")

        # Get products from Product Aware API
        products = product_aware_service.get_batch_products(product_names)

        return jsonify({
            'success': True,
            'products': products,
            'count': len(products)
        })

    except Exception as e:
        logger.error(f"❌ Error in batch products endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Failed to fetch batch products: {str(e)}'
        }), 500

@app.route('/api/products/search', methods=['POST'])
def search_products():
    """Search for products by name"""
    try:
        data = request.get_json()
        query = data.get('query', '')

        if not query:
            return jsonify({'error': 'No search query provided'}), 400

        logger.info(f"🔍 Product search request: '{query}'")

        # Search products
        products = product_aware_service.search_products(query)

        return jsonify({
            'success': True,
            'products': products,
            'count': len(products),
            'query': query
        })

    except Exception as e:
        logger.error(f"❌ Error in product search endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Failed to search products: {str(e)}'
        }), 500

def is_valid_product(product: dict) -> bool:
    """
    Validate product data to exclude invalid/test entries

    Filters out products with:
    - Country names as manufacturer (China, USA, India, etc.)
    - City names as product names (Paris, London, etc.)
    - Generic/placeholder names
    - Missing essential data
    """
    # List of country names that shouldn't be manufacturers
    invalid_manufacturers = {
        'China', 'USA', 'United States', 'UK', 'United Kingdom', 'Europe',
        'Asia', 'America', 'India', 'Japan', 'Germany', 'France', 'Italy',
        'Spain', 'Canada', 'Brazil', 'Australia', 'Russia', 'Korea',
        'Netherlands', 'Belgium', 'Switzerland', 'Austria', 'Sweden', 'Norway',
        'Denmark', 'Finland', 'Poland', 'Portugal', 'Greece', 'Ireland',
        'Singapore', 'Malaysia', 'Thailand', 'Vietnam', 'Indonesia', 'Philippines'
    }

    # List of city names that shouldn't be product names
    invalid_product_names = {
        'Paris', 'London', 'New York', 'Tokyo', 'Sydney', 'Berlin', 'Rome',
        'Madrid', 'Beijing', 'Shanghai', 'Mumbai', 'Dubai', 'Los Angeles',
        'Chicago', 'Houston', 'Toronto', 'Vancouver', 'Melbourne', 'Brisbane'
    }

    manufacturer = product.get('manufacturer_name', '').strip()
    product_name = product.get('product_name', '').strip()

    # Check if manufacturer is a country name
    if manufacturer in invalid_manufacturers:
        logger.debug(f"Filtered out product with invalid manufacturer: {manufacturer} - {product_name}")
        return False

    # Check if product name is a city name
    if product_name in invalid_product_names:
        logger.debug(f"Filtered out product with invalid name: {product_name} (manufacturer: {manufacturer})")
        return False

    # Filter out products with missing essential information
    if not manufacturer or not product_name:
        return False

    # Filter out placeholder names
    if manufacturer.lower() in ['unknown', 'test', 'sample', 'demo', 'placeholder']:
        return False

    if product_name.lower() in ['unknown', 'test', 'sample', 'demo', 'placeholder']:
        return False

    return True


@app.route('/api/products/all', methods=['GET'])
def get_all_products_endpoint():
    """Get all products with basic info for browsing"""
    try:
        logger.info("📦 Fetching all products for landing page")

        # Wait for cache if it's still loading
        import time
        wait_count = 0
        while product_aware_service.cache_loading and wait_count < 30:
            time.sleep(0.5)
            wait_count += 1

        # Get all products from cache
        all_products = []
        seen_ids = set()
        filtered_count = 0

        for products_list in product_aware_service.fast_search_cache.values():
            for product in products_list:
                product_id = product.get('id')
                # Avoid duplicates
                if product_id and product_id not in seen_ids:
                    seen_ids.add(product_id)

                    # Validate product data quality
                    if not is_valid_product(product):
                        filtered_count += 1
                        continue

                    # Use manufacturer_name as the category for filtering
                    manufacturer = product.get('manufacturer_name', 'Unknown')

                    # Convert image path to S3 URL (same as CODE2120-W8)
                    image_path = product.get('image', '')
                    image_url = ''
                    if image_path:
                        if image_path.startswith('products/'):
                            image_url = f'https://architectsdeclareapp.s3.amazonaws.com/media/{image_path}'
                        elif image_path.startswith('/products/'):
                            image_url = f'https://architectsdeclareapp.s3.amazonaws.com/media{image_path}'
                        else:
                            image_url = image_path

                    all_products.append({
                        'id': product.get('id'),
                        'product_name': product.get('product_name', 'Unknown'),
                        'product_code': product.get('product_code', ''),
                        'manufacturer_name': manufacturer,
                        'product_description': product.get('product_description', ''),
                        'brand': product.get('brand', ''),
                        'category': manufacturer,  # Use manufacturer as category
                        'image_url': image_url,
                        'thumbnail_url': image_url,
                    })

        logger.info(f"✅ Returning {len(all_products)} valid products (filtered out {filtered_count} invalid entries)")

        return jsonify({
            'success': True,
            'products': all_products,
            'count': len(all_products),
            'filtered_count': filtered_count
        })

    except Exception as e:
        logger.error(f"❌ Error in get all products endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Failed to fetch all products: {str(e)}'
        }), 500

@app.route('/api/products/categories', methods=['GET'])
def get_product_categories():
    """Get hierarchical product categories (major categories + subcategories)"""
    try:
        logger.info("📂 Fetching product categories")

        # Define major product categories for AEC industry
        # Mapping manufacturer/product keywords to major categories
        category_mapping = {
            'Building Materials': ['brick', 'concrete', 'cement', 'mortar', 'plaster', 'csr', 'boral', 'brickworks', 'austral'],
            'Tiles & Flooring': ['tile', 'porcelain', 'ceramic', 'kaolin', 'floor', 'paving', 'mosaic'],
            'Wall & Ceiling': ['wall', 'ceiling', 'panel', 'board', 'sheet', 'drywall', 'partition'],
            'Roofing & Cladding': ['roof', 'clad', 'gutter', 'fascia', 'ridge', 'colorbond'],
            'Insulation & Waterproofing': ['insulation', 'thermal', 'waterproof', 'membrane', 'sealant'],
            'Windows & Doors': ['window', 'door', 'frame', 'glass', 'glazing'],
            'Hardware & Fixtures': ['handle', 'hinge', 'lock', 'fixture', 'fitting', 'hardware'],
            'Paints & Coatings': ['paint', 'coating', 'finish', 'stain', 'varnish', 'render'],
            'Structural Steel': ['steel', 'beam', 'column', 'truss', 'infrabuild', 'onesteel'],
            'Timber & Wood Products': ['timber', 'wood', 'lumber', 'ply', 'mdf', 'particle'],
            'Electrical & Lighting': ['electrical', 'light', 'cable', 'switch', 'socket'],
            'Plumbing & Drainage': ['pipe', 'plumb', 'drain', 'valve', 'tap', 'faucet'],
        }

        # Collect all manufacturers and their products (only genuine products with images)
        manufacturer_products = {}
        all_manufacturers = set()

        for products_list in product_aware_service.fast_search_cache.values():
            for product in products_list:
                # Skip invalid products
                if not is_valid_product(product):
                    continue

                manufacturer = product.get('manufacturer_name', '').strip()
                product_name = product.get('product_name', '').lower()

                if manufacturer:
                    all_manufacturers.add(manufacturer)
                    if manufacturer not in manufacturer_products:
                        manufacturer_products[manufacturer] = []
                    manufacturer_products[manufacturer].append(product_name)

        # Categorize manufacturers into major categories
        categorized = {cat: set() for cat in category_mapping.keys()}
        uncategorized = set()

        for manufacturer in all_manufacturers:
            manufacturer_lower = manufacturer.lower()
            categorized_flag = False

            # Check manufacturer name and their products against keywords
            for major_cat, keywords in category_mapping.items():
                # Check manufacturer name
                if any(keyword in manufacturer_lower for keyword in keywords):
                    categorized[major_cat].add(manufacturer)
                    categorized_flag = True
                    break

                # Check product names from this manufacturer
                if manufacturer in manufacturer_products:
                    product_names = ' '.join(manufacturer_products[manufacturer])
                    if any(keyword in product_names for keyword in keywords):
                        categorized[major_cat].add(manufacturer)
                        categorized_flag = True
                        break

            if not categorized_flag:
                uncategorized.add(manufacturer)

        # Add uncategorized to "Other Building Products"
        if uncategorized:
            categorized['Other Building Products'] = uncategorized

        # Remove empty categories
        categorized = {k: sorted(list(v)) for k, v in categorized.items() if v}

        logger.info(f"✅ Found {len(categorized)} major categories")
        logger.info(f"   Major categories: {list(categorized.keys())}")

        return jsonify({
            'success': True,
            'hierarchical': categorized,  # { 'Major Category': ['Manufacturer1', 'Manufacturer2', ...] }
            'major_categories': sorted(list(categorized.keys())),
            'all_manufacturers': sorted(list(all_manufacturers))
        })

    except Exception as e:
        logger.error(f"❌ Error in get categories endpoint: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': f'Failed to fetch categories: {str(e)}',
            'hierarchical': {},
            'major_categories': ['All Products'],
            'all_manufacturers': []
        }), 200  # Return 200 with defaults to not break frontend

# Risk assessment routes
@app.route('/api/risk/assess', methods=['POST'])
def assess_risk():
    """Assess supply chain risk for given products"""
    try:
        data = request.get_json()
        product_names = data.get('productNames', [])

        if not product_names:
            return jsonify({'error': 'No product names provided'}), 400

        logger.info(f"⚠️ Risk assessment request for {len(product_names)} products")

        # Get product data
        products = product_aware_service.get_batch_products(product_names)

        if not products:
            return jsonify({
                'success': False,
                'error': 'No valid products found for risk assessment'
            }), 404

        # Process each product individually for multi-product analysis
        product_results = []

        for product in products:
            # Extract locations for this specific product
            product_locations = product_aware_service.extract_locations(product)

            if not product_locations:
                logger.warning(f"⚠️ No locations found for product: {product.get('name')}")
                continue

            # Geocode locations for this product
            geocoded_product_locations = []
            for location in product_locations:
                try:
                    geocoded = geocoding_service.geocode_location(location)
                    if geocoded:
                        geocoded_product_locations.append(geocoded)
                except Exception as e:
                    logger.warning(f"⚠️ Failed to geocode location {location.get('address', 'Unknown')}: {str(e)}")
                    geocoded_product_locations.append(location)

            # Perform risk assessment for this product
            risk_results = risk_assessment_service.assess_supply_chain_risk(geocoded_product_locations, [product])

            # Transform assessment for this product
            geo = risk_results.get('geographic_risk', {})
            conc = risk_results.get('concentration_risk', {})

            # Convert HHI (0-1) to 0-10000 scale
            hhi_0_1 = geo.get('hhi', 0.0)
            hhi_scaled = int(round(hhi_0_1 * 10000))
            concentration_level = geo.get('concentration_risk', conc.get('concentration_level', 'low'))

            # Lead time for this product
            lt = risk_results.get('lead_time_risk', {'average_risk': 0.0, 'items': []})
            total_percent = int(round(risk_results.get('risk_score', 0.0) * 100))

            # Build product result
            product_result = {
                'id': product.get('id'),
                'name': product.get('name'),
                'code': product.get('code'),
                'manufacturer': product.get('manufacturer'),
                'description': product.get('description'),
                'locations': geocoded_product_locations,
                'manufacturingSites': product.get('manufacturingSites', []),
                'rawMaterialSources': product.get('rawMaterialSources', []),
                'suppliers': product.get('suppliers', []),
                'assessment': {
                    'overallRiskLevel': risk_results.get('overall_risk', 'UNKNOWN'),
                    'totalRiskPercentage': total_percent,
                    'hhi': {
                        'score': hhi_scaled,
                        'riskLevel': concentration_level,
                        'bySegment': geo.get('hhi_by_segment', {})
                    },
                    'leadTime': {
                        'riskPercentage': int(round(lt.get('average_risk', 0.0) * 100)),
                        'status': lt.get('items', [{}])[0].get('status', 'Unknown') if lt.get('items') else 'Unknown',
                        'riskLevel': lt.get('items', [{}])[0].get('risk_level', 'UNKNOWN') if lt.get('items') else 'UNKNOWN'
                    },
                    'geographic': {
                        'riskScore': int(round(geo.get('risk_score', 0.0) * 100))
                    },
                    'climate': {
                        'riskScore': int(round(risk_results.get('climate_risk', {}).get('risk_score', 0.0) * 100))
                    },
                    'geopolitical': {
                        'riskScore': int(round(risk_results.get('geopolitical_risk', {}).get('risk_score', 0.0) * 100))
                    }
                }
            }

            product_results.append(product_result)
            logger.info(f"✅ Completed analysis for {product.get('name')}")

        if not product_results:
            return jsonify({
                'success': False,
                'error': 'No products could be analyzed successfully'
            }), 404

        return jsonify({
            'success': True,
            'products': product_results,
            'products_analyzed': len(product_results)
        })

    except Exception as e:
        logger.error(f"❌ Error in risk assessment endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Failed to assess supply chain risk: {str(e)}'
        }), 500


@app.route('/api/chat', methods=['POST'])
def chat_assistant():
    """Conversational assistant that explains current assessment"""
    if openai_client is None:
        logger.warning("Chat assistant requested but openai_client is None - check OPENAI_API_KEY")
        return jsonify({
            'success': False,
            'error': 'Chat assistant not configured. Please set OPENAI_API_KEY on the server.'
        }), 503

    try:
        payload = request.get_json() or {}
        # expected structure: { messages: [...], assessment: {...} }
        messages = payload.get('messages') or []
        assessment = payload.get('assessment') or {}
        all_products = assessment.get('allProducts', [])

        logger.info(f"💬 Chat request received:")
        logger.info(f"   - Messages: {len(messages)}")
        logger.info(f"   - Assessment keys: {list(assessment.keys())}")
        logger.info(f"   - All products count: {len(all_products)}")
        if all_products:
            product_names = [p.get('name', 'Unknown') for p in all_products]
            logger.info(f"   - Product names ({len(product_names)}): {product_names}")
            # Log first product structure for debugging
            if all_products[0]:
                first_prod = all_products[0]
                logger.info(f"   - First product structure: name={first_prod.get('name')}, has_assessment={'assessment' in first_prod}")
                if 'assessment' in first_prod:
                    logger.info(f"   - First product assessment keys: {list(first_prod.get('assessment', {}).keys())}")
                    logger.info(f"   - First product totalRisk: {first_prod.get('assessment', {}).get('totalRiskPercentage', 'NOT FOUND')}")

        # Sanitize history: only allow role/content keys with safe roles
        allowed_roles = {'user', 'assistant'}
        sanitized_history = [
            {'role': msg.get('role'), 'content': msg.get('content', '')[:2000]}
            for msg in messages
            if isinstance(msg, dict) and msg.get('role') in allowed_roles and msg.get('content')
        ][-10:]  # limit conversation depth

        # Build detailed context with explicit breakdown
        hhi_data = assessment.get('hhi', {})
        lead_time_data = assessment.get('leadTime', {})
        total_risk = assessment.get('totalRiskPercent', 0)

        # Format HHI percentage (0-10000 scale to 0-100%)
        hhi_value = hhi_data.get('hhi', 0)
        hhi_percent = round((hhi_value / 10000) * 100) if hhi_value else 0

        # Format lead time risk percentage
        lead_time_risk = round((lead_time_data.get('averageRisk', 0)) * 100)

        # Extract segmented HHI data
        by_segment = hhi_data.get('bySegment', {})
        materials_to_mfg = by_segment.get('materials_to_manufacturing', {})
        mfg_to_storage = by_segment.get('manufacturing_to_storage', {})

        # Build products summary with detailed information
        products_summary = []
        detailed_products_list = []

        for prod in all_products:
            prod_assessment = prod.get('assessment', {})
            hhi_data_prod = prod_assessment.get('hhi', {})
            lead_time_data_prod = prod_assessment.get('leadTime', {})

            # Summary for quick reference
            products_summary.append({
                'name': prod.get('name', 'Unknown'),
                'totalRisk': prod_assessment.get('totalRiskPercentage', 0),
                'riskLevel': prod_assessment.get('overallRiskLevel', 'UNKNOWN'),
                'hhiScore': round((hhi_data_prod.get('score', 0) / 100)) if hhi_data_prod.get('score') else 0,
                'leadTimeRisk': lead_time_data_prod.get('riskPercentage', 0)
            })

            # Detailed information for each product
            hhi_score_prod = hhi_data_prod.get('score', 0)
            hhi_percent_prod = round((hhi_score_prod / 100)) if hhi_score_prod else 0
            total_risk_prod = prod_assessment.get('totalRiskPercentage', 0)

            detailed_products_list.append({
                'name': prod.get('name', 'Unknown'),
                'code': prod.get('code', ''),
                'manufacturer': prod.get('manufacturer', ''),
                'totalRisk': total_risk_prod,
                'riskLevel': prod_assessment.get('overallRiskLevel', 'UNKNOWN'),
                'hhi': {
                    'score': hhi_percent_prod,
                    'riskLevel': hhi_data_prod.get('riskLevel', 'UNKNOWN')
                },
                'leadTime': {
                    'riskPercentage': lead_time_data_prod.get('riskPercentage', 0),
                    'status': lead_time_data_prod.get('status', 'Unknown')
                },
                'calculation': f"Total Risk = ({hhi_percent_prod}% × 0.7) + ({lead_time_data_prod.get('riskPercentage', 0)}% × 0.3) = {total_risk_prod}%"
            })

        # Build context with all products - PUT THIS FIRST AND MAKE IT VERY EXPLICIT
        if len(all_products) > 0:
            # Create a simple text list of products for easy searching
            product_list_text = "\n".join([f"- {p.get('name', 'Unknown')}: {prod_assess.get('totalRiskPercentage', 0)}% total risk"
                                          for p, prod_assess in zip(all_products, [p.get('assessment', {}) for p in all_products])])

            products_section = f"""=== ANALYZED PRODUCTS ({len(all_products)} total) ===

PRODUCT LIST (Search for product names here):
{product_list_text}

DETAILED PRODUCT DATA (Each product's complete risk information):
{json.dumps(detailed_products_list, indent=2)}

IMPORTANT: When a user asks about a specific product (e.g., "Kooltherm K17"), search for that product name in the DETAILED PRODUCT DATA section above and use its data to answer the question."""
        else:
            products_section = "=== NO PRODUCTS ANALYZED ===\nNo products have been analyzed yet."

        detailed_context = f"""{products_section}

=== SELECTED PRODUCT DETAILS ===
- Name: {assessment.get('productName', 'Not selected')}
- Overall Risk: {total_risk}%
- Risk Level: {assessment.get('overallRisk', 'unknown').upper()}

RISK CALCULATION FORMULA:
- Total Risk = (HHI Risk × 70%) + (Lead Time Risk × 30%)
- Total Risk = ({hhi_percent}% × 0.7) + ({lead_time_risk}% × 0.3) = {total_risk}%

HHI (Geographic Concentration) ANALYSIS:
- HHI Score: {hhi_percent}% (on 0-100% scale)
- Risk Level: {hhi_data.get('riskLevel', 'unknown')}
- Interpretation: {hhi_data.get('interpretation', 'N/A')}
- Country Distribution: {json.dumps(hhi_data.get('countryDistribution', {}), indent=2)}

SEGMENTED HHI BREAKDOWN:
1. Raw Materials → Manufacturing:
   - Base HHI: {round((materials_to_mfg.get('base_hhi', 0)) * 100)}%
   - Adjusted HHI: {round((materials_to_mfg.get('adjusted_hhi', 0)) * 100)}%
   - Same-country Overlap: {round((materials_to_mfg.get('overlap_ratio', 0)) * 100)}%

2. Manufacturing → Storage/Distribution:
   - Base HHI: {round((mfg_to_storage.get('base_hhi', 0)) * 100)}%
   - Adjusted HHI: {round((mfg_to_storage.get('adjusted_hhi', 0)) * 100)}%
   - Same-country Overlap: {round((mfg_to_storage.get('overlap_ratio', 0)) * 100)}%

LEAD TIME RISK ANALYSIS:
- Average Lead Time Risk: {lead_time_risk}%
- Individual Products: {json.dumps(lead_time_data.get('items', [])[:5], indent=2)}

RECOMMENDATIONS:
{json.dumps(assessment.get('recommendations', []), indent=2)}

Use this detailed breakdown to answer questions about the risk assessment. When comparing products, refer to the ALL ANALYZED PRODUCTS section. Always cite specific numbers and explain calculations."""

        system_prompt = {
            'role': 'system',
            'content': (
                'You are a supply chain risk analyst. Answer questions using ONLY the provided assessment data. '
                '\n\nCRITICAL INSTRUCTIONS FOR FINDING PRODUCT DATA:\n'
                '1. The context includes a "=== ANALYZED PRODUCTS ===" section at the TOP that lists ALL analyzed products.\n'
                '2. The "DETAILED PRODUCT DATA" section contains COMPLETE information for each product including:\n'
                '   - name: The product name (e.g., "Kooltherm K17")\n'
                '   - totalRisk: The total risk percentage (e.g., 45)\n'
                '   - riskLevel: The risk level (e.g., "MEDIUM")\n'
                '   - hhi: Object with score (percentage) and riskLevel\n'
                '   - leadTime: Object with riskPercentage and status\n'
                '   - calculation: The exact calculation formula showing how totalRisk was computed\n'
                '3. When a user asks about a specific product by name (e.g., "Kooltherm K17" or "why is the total risk of Kooltherm K17 45%?"):\n'
                '   a) FIRST, search for that product name in the DETAILED PRODUCT DATA section\n'
                '   b) Find the matching product object (check the "name" field)\n'
                '   c) Use ALL the data from that product object to answer\n'
                '   d) Quote the exact values: totalRisk, hhi.score, leadTime.riskPercentage\n'
                '   e) Use the "calculation" field to show how the totalRisk was computed\n'
                '4. Example: If asked "why is the total risk of Kooltherm K17 45%?", find the product with name="Kooltherm K17" in DETAILED PRODUCT DATA and explain using its hhi.score, leadTime.riskPercentage, and calculation fields.\n'
                '5. When comparing products, list each product with its specific totalRisk, hhi.score, and leadTime.riskPercentage from DETAILED PRODUCT DATA.\n'
                '\nFORMATTING:\n'
                '- Use plain text only. NO LaTeX, markdown math, or special characters.\n'
                '- Write formulas like: "Total Risk = (HHI × 70%) + (Lead Time × 30%)"\n'
                '- Use × for multiply, = for equals, % for percentages.\n'
                '\nIf a product name is not found in DETAILED PRODUCT DATA, say "I cannot find [product name] in the analyzed products. Available products are: [list names from PRODUCT LIST]".'
            )
        }

        context_prompt = {
            'role': 'system',
            'content': detailed_context
        }

        # Log context preview for debugging (first 1000 chars)
        context_preview = detailed_context[:1000] if len(detailed_context) > 1000 else detailed_context
        logger.info(f"   - Context preview (first 1000 chars):\n{context_preview}...")

        chat_response = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[system_prompt, context_prompt] + sanitized_history,
            temperature=0.3,
            max_tokens=500
        )

        reply = chat_response.choices[0].message.content
        return jsonify({'success': True, 'reply': reply})

    except Exception as exc:
        logger.error(f"Error in chat assistant endpoint: {exc}")
        return jsonify({
            'success': False,
            'error': 'Chat assistant failed to generate a response.'
        }), 500

# Data quality routes
@app.route('/api/data-quality', methods=['POST'])
def get_data_quality():
    """Get data quality metrics for given products"""
    try:
        data = request.get_json()
        product_names = data.get('productNames', [])

        if not product_names:
            return jsonify({'error': 'No product names provided'}), 400

        logger.info(f"📊 Data quality request for {len(product_names)} products")

        # Get product data
        products = product_aware_service.get_batch_products(product_names)

        if not products:
            return jsonify({
                'success': False,
                'error': 'No valid products found for data quality analysis'
            }), 404

        # Calculate data quality metrics
        quality_metrics = product_aware_service.calculate_data_quality(products)

        return jsonify({
            'success': True,
            'data_quality': quality_metrics,
            'products_analyzed': len(products)
        })

    except Exception as e:
        logger.error(f"❌ Error in data quality endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Failed to calculate data quality: {str(e)}'
        }), 500

# Serve frontend React app
@app.route('/')
def serve_frontend():
    """Serve the React frontend"""
    try:
        return render_template('index.html')
    except Exception as e:
        logger.error(f"Error serving frontend: {str(e)}")
        return jsonify({
            'message': 'Supply Chain Risk Analysis Flask Backend',
            'version': '1.0.0',
            'status': 'running',
            'error': f'Frontend template not found: {str(e)}',
            'endpoints': {
                'health': '/api/health',
                'products_batch': '/api/products/batch',
                'products_search': '/api/products/search',
                'risk_assess': '/api/risk/assess',
                'data_quality': '/api/data-quality'
            }
        })

# Catch-all route for React Router (must be after API routes)
@app.route('/<path:path>')
def serve_frontend_any(path):
    """Serve React app for client-side routing (catch-all after API routes)"""
    # Don't interfere with API routes
    if path.startswith('api/'):
        return jsonify({'error': 'API endpoint not found'}), 404

    # Serve index.html for all other routes (React Router will handle client-side routing)
    try:
        return render_template('index.html')
    except Exception as e:
        logger.error(f"Error serving frontend route {path}: {str(e)}")
        return jsonify({'error': 'Frontend not available'}), 500

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'

    logger.info(f"Starting Flask server on port {port}")
    logger.info(f"Debug mode: {debug}")

    app.run(host='0.0.0.0', port=port, debug=debug)
