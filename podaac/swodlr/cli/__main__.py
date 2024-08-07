from argparse import ArgumentParser
from importlib import resources
import json
from os import environ
from pathlib import Path
import gql
from gql.transport.aiohttp import AIOHTTPTransport
from earthaccess import api as earthaccessapi

SWODLR_ENVIRONMENTS = {
    'ops': 'https://swodlr.podaac.earthdatacloud.nasa.gov/api/graphql',
    'uat': 'https://swodlr.podaac.uat.earthdatacloud.nasa.gov/api/graphql',
    'sit': 'https://swodlr.podaac.sit.earthdatacloud.nasa.gov/api/graphql'
}

def main():
    parser = ArgumentParser('swodlr')
    parser.add_argument('--env',
                        help='The SWODLR environment to run commands against',
                        default='ops'
                        )
    
    subparsers = parser.add_subparsers()

    # -- get-users-products
    get_users_products_parser = subparsers.add_parser(
        'get-users-products',
        help='Get a user\'s products by their EDL username; optionally apply '
        + 'filters to the query'
    )
    get_users_products_parser.set_defaults(func=get_users_products)
    get_users_products_parser.add_argument('username')
    get_users_products_parser.add_argument('--cycle')
    get_users_products_parser.add_argument('--pass')
    get_users_products_parser.add_argument('--scene')
    get_users_products_parser.add_argument('--output-granule-extent-flag')
    get_users_products_parser.add_argument('--output-sampling-grid-type')
    get_users_products_parser.add_argument('--raster-resolution')
    get_users_products_parser.add_argument('--utm-zone-adjust')
    get_users_products_parser.add_argument('--mgrs-band-adjust')
    get_users_products_parser.add_argument('--before-timestamp')
    get_users_products_parser.add_argument('--after-timestamp')
    get_users_products_parser.add_argument(
        '--after-id',
        help='Used for pagination; the last product id from the last page'
    )
    get_users_products_parser.add_argument(
        '--limit',
        help='The number of results to return',
        default=10
    )
    
    # -- invalidate-product
    invalidate_product_parser = subparsers.add_parser(
        'invalidate-product',
        help='Invalidate a product by product id; allows the product to be ' +
        'regenerated regardless of current product status by transitioning ' +
        'the product to UNAVAILABLE'
    )
    invalidate_product_parser.set_defaults(func=invalidate_product)
    invalidate_product_parser.add_argument('id')
    
    # -- search-products
    search_products_parser = subparsers.add_parser(
        'search-products',
        help='Search for existing products in the cache without generating ' +
        'any new products'
    )
    search_products_parser.set_defaults(func=search_products)
    search_products_parser.add_argument('--cycle')
    search_products_parser.add_argument('--pass')
    search_products_parser.add_argument('--scene')
    search_products_parser.add_argument('--output-granule-extent-flag')
    search_products_parser.add_argument('--output-sampling-grid-type')
    search_products_parser.add_argument('--raster-resolution')
    search_products_parser.add_argument('--utm-zone-adjust')
    search_products_parser.add_argument('--mgrs-band-adjust')
    search_products_parser.add_argument(
        '--after-id',
        help='Used for pagination; the last product id from the last page'
    )
    invalidate_product_parser.add_argument(
        '--limit',
        help='The number of results to return',
        default=10
    )
    
    args = parser.parse_args()
    
    if not hasattr(args, 'func'):
        parser.print_help()
        return

    # Configure client before passing to function
    token = earthaccessapi.login().token
    transport = AIOHTTPTransport(
        url=_get_graphql_url(args.env),
        headers={'Authorization': f'Bearer {token['access_token']}'}
    )
    session = gql.Client(transport=transport)
    func = args.func
        
    # Remove misc params and convert to dict
    args = vars(args)
    for key in ['func', 'env']:
        if key in args:
            del args[key]

    func(session, args)


def get_users_products(session, args):
    query = _load_graphql_query('get_users_products')
    results = session.execute(query, variable_values=args)
    print(json.dumps(results, indent=2))


def invalidate_product(session, args):
    query = _load_graphql_query('invalidate_product')
    results = session.execute(query, variable_values=args)
    print(json.dumps(results, indent=2))


def search_products(session, args):
     query = _load_graphql_query('search_products')
     results = session.execute(query, variable_values=args)
     print(json.dumps(results, indent=2))


def _load_graphql_query(name):
    path = resources.files().joinpath('graphql-documents', f'{name}.graphql')

    if not path.is_file():
        raise RuntimeError(f'GraphQL document not found: {str(path)}')

    return gql.gql(path.read_text('utf-8'))


def _get_graphql_url(env = None):
    # Check for environmental overrides first
    url = environ.get('SWODLR_CLI_URL')
    if url is not None:
        return url
    
    venue = environ.get('SWODLR_CLI_ENV', env)

    if venue is None:
        raise RuntimeError(f'Invalid environment: {venue}')
    
    venue = venue.lower()
    if venue not in SWODLR_ENVIRONMENTS:
        raise RuntimeError(f'Invalid environment: {venue}')
    
    return SWODLR_ENVIRONMENTS[venue]

if __name__ == '__main__':
    main()
