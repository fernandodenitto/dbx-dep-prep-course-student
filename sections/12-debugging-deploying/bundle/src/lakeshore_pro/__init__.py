"""lakeshore_pro: the course's shared transforms, in one package.

The dots below are relative imports. They mean "from this package" and they work
wherever the package lands: a Git folder, a bundle's deployed files, or a wheel.
"""

from .orders import add_cart_total, parse_bus_orders, reject_reason
from .privacy import pseudonymize_email

__version__ = "0.1.0"
