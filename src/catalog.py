"""Reference data shared by the generator and the pipeline."""

# city, state, region, relative order volume
CITIES = [
    ("Mumbai", "Maharashtra", "West", 14),
    ("Delhi", "Delhi", "North", 14),
    ("Bengaluru", "Karnataka", "South", 13),
    ("Hyderabad", "Telangana", "South", 9),
    ("Chennai", "Tamil Nadu", "South", 8),
    ("Kolkata", "West Bengal", "East", 7),
    ("Pune", "Maharashtra", "West", 7),
    ("Ahmedabad", "Gujarat", "West", 5),
    ("Jaipur", "Rajasthan", "North", 4),
    ("Lucknow", "Uttar Pradesh", "North", 4),
    ("Kochi", "Kerala", "South", 3),
    ("Chandigarh", "Chandigarh", "North", 3),
    ("Indore", "Madhya Pradesh", "Central", 3),
    ("Bhopal", "Madhya Pradesh", "Central", 2),
    ("Patna", "Bihar", "East", 2),
    ("Mangaluru", "Karnataka", "South", 2),
]

# Messy spellings that real source systems produce. The Silver layer maps them back.
CITY_VARIANTS = {
    "Bengaluru": ["bangalore", "Bangalore", "BENGALURU ", " Bengaluru", "BLR"],
    "Mumbai": ["Bombay", "mumbai", "MUMBAI"],
    "Delhi": ["New Delhi", "delhi", "NCR Delhi"],
    "Kolkata": ["Calcutta", "kolkata"],
    "Chennai": ["Madras", "chennai"],
    "Mangaluru": ["Mangalore"],
    "Hyderabad": ["hyderabad", "Hyd"],
    "Pune": ["Poona", "pune"],
}

# category -> subcategory -> (min price, max price) in INR
CATALOG = {
    "Electronics": {
        "Smartphones": (8000, 60000), "Phone Cases": (199, 999), "Screen Guards": (149, 599),
        "Laptops": (30000, 120000), "Mouse": (299, 2499), "Laptop Bags": (699, 2999),
        "Headphones": (499, 15000), "Power Banks": (799, 3999), "Smart Watches": (1499, 25000),
    },
    "Fashion": {
        "T-Shirts": (299, 1499), "Jeans": (799, 3499), "Shoes": (999, 7999),
        "Socks": (99, 499), "Sunglasses": (499, 4999), "Handbags": (799, 5999),
    },
    "Home & Kitchen": {
        "Cookware": (599, 4999), "Kitchen Tools": (149, 999), "Bedsheets": (499, 2999),
        "Pillows": (299, 1499), "Storage Boxes": (199, 1299), "Mixer Grinders": (1999, 7999),
    },
    "Beauty": {
        "Face Wash": (99, 599), "Moisturizer": (199, 999), "Shampoo": (149, 799),
        "Conditioner": (149, 799), "Perfume": (399, 4999),
    },
    "Books": {
        "Fiction": (149, 599), "Exam Prep": (299, 999), "Self Help": (199, 699), "Bookmarks": (49, 199),
    },
    "Sports": {
        "Yoga Mats": (399, 1999), "Dumbbells": (499, 3999), "Cricket Bats": (799, 9999),
        "Cricket Balls": (99, 599), "Water Bottles": (149, 899),
    },
    "Grocery": {
        "Tea": (99, 599), "Biscuits": (20, 199), "Coffee": (149, 999), "Snacks": (20, 299), "Dry Fruits": (299, 1999),
    },
    "Toys": {
        "Board Games": (299, 2499), "Puzzles": (199, 999), "Action Figures": (299, 1999), "Batteries": (49, 399),
    },
}

# Products that are bought together in real life. The generator plants these patterns;
# market basket analysis in the pipeline must rediscover them from the data.
COMPLEMENTS = {
    "Smartphones": ["Phone Cases", "Screen Guards", "Power Banks"],
    "Laptops": ["Mouse", "Laptop Bags"],
    "Shampoo": ["Conditioner"],
    "Face Wash": ["Moisturizer"],
    "Cricket Bats": ["Cricket Balls"],
    "Jeans": ["T-Shirts"],
    "Shoes": ["Socks"],
    "Tea": ["Biscuits"],
    "Coffee": ["Snacks", "Biscuits"],
    "Yoga Mats": ["Water Bottles"],
    "Dumbbells": ["Water Bottles"],
    "Cookware": ["Kitchen Tools"],
    "Bedsheets": ["Pillows"],
    "Fiction": ["Bookmarks"],
    "Exam Prep": ["Bookmarks"],
    "Action Figures": ["Batteries"],
    "Board Games": ["Puzzles"],
}

# Fictional brand names
BRANDS = {
    "Electronics": ["Voltix", "Nexora", "Zentek", "Orbis", "Pixelon"],
    "Fashion": ["UrbanThread", "Desi Denim", "StrideCo", "Mehra & Sons", "Kaafi"],
    "Home & Kitchen": ["GharCraft", "KitchenKing", "SoftNest", "Tiffinly"],
    "Beauty": ["GlowVeda", "PureLeaf", "Aromaa", "SkinSutra"],
    "Books": ["Pustak Press", "Inkwell", "Gyaan Books"],
    "Sports": ["FitBharat", "BoundaryPro", "Kridaa"],
    "Grocery": ["Chai Point Co", "Annapurna Foods", "Munchies", "Kesar Farms"],
    "Toys": ["PlayPal", "Khilona", "BrainBox"],
}
