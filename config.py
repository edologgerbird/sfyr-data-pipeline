
'''
TickerExtractor
'''

word_mapper = {"Intl": "Int", "intl": "int",
               "YZJ Shipbldg SGD": "Yangzijiang Shipbuilding", "YZJ": "Yangzijiang", " SPAC": "",
               "Reit": "REIT", "Singtel": "Singapore Telecommunications ",
               "SIA": "Singapore Airlines", "STI": "Straits Time Index",
               "OCBC Bank": "OCBC", "CapitaLandInvest": "CapitaLand Invest",
               "JMH": "Jardine Matheson", "Tianjin ZX": "Tianjin Zhongxin",
               "SingPost": "Singapore Post", "SingtelMBeCW220915": "Singtel",
               "Nomura Yen1k": "Nomura", " USD": "", " SGD": "", " US$": "", " SG$": ""}

exclusion = ["Singapore", "Sing", "United", "Invest",
             "Energy", "Investing", "SG", "US", "SGD", "USD", "Top", "Union", "Second", "World", "Asia", "Europe", "America", "Africa", "China", "Korea", "Straits"]


#  r'[A-Z0-9](?:[a-z&]*|[A-Z]*(?=[A-Z]|$)*)'
