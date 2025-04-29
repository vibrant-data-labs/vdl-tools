#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 12 14:54:17 2021

@author: ericberlow
"""


########################################
########################################
                    
##############################
# OTHER LISTS/DICTS 


# map funding types
fundingDict = {'Grant': 'Grant', 
               'Pre-Seed': 'Pre-Seed', 
                
                'Seed':'Seed', 
                'Angel': 'Seed', 
                'Initial Coin Offering': 'ICO', 
                'Equity Crowdfunding': 'Crowdfunding', 
                'Product Crowdfunding': 'Crowdfunding', 

                'Series A':  'Early Venture', #'Series A',
                'Series B':  'Early Venture', #'Series B',

                'Series C':  'Late Venture', #'Series C',
                'Venture - Series Unknown': 'Venture (Unknown Stage)', 
                'Corporate Round': 'Corporate Round', #'Venture-Mid', 
                
                'M&A' : 'M&A',

                'Series D': 'Late Venture', 
                'Series E': 'Late Venture', 
                'Series F': 'Late Venture', 
                'Series G': 'Late Venture', 
                'Series H': 'Late Venture', 

                'Private Equity': 'Private Equity' , 
                'Secondary Market': 'Private Equity', 

                'Post-IPO Debt': 'Post-IPO', 
                'Post-IPO Secondary': 'Post-IPO', 
                'Post-IPO Equity': 'Post-IPO', 

                'Debt Financing': 'Debt', 
                'Non-equity Assistance': 'Non-Equity', 
                'Convertible Note': 'Convertible Note', 
                'Funding Round': 'Venture (Unknown Stage)', 
                }

stageDict = {'Grant': 0,
             
             'Pre-Seed': 1,
             'Convertible Note': 1,
             'Seed': 2,
             'Angel': 2,
             'ICO': 2,
             'Crowdfunding': 2,
             'Series A': 3,
             'Early Venture': 3,
             'Series B': 4, 
             'Venture (Unknown Stage)': 4,
             
             'Corporate Round': 5,
             'Series C': 6,
             'M&A': 6, 
             'Late Venture': 7,
             'Private Equity': 8,
             'IPO': 9,
             'Post-IPO': 10   ,
             
             'Debt': 15,
             'Non-Equity': 15,           
            }

stageCategoriesDict = {0: "Grants",
                   1: "Early Venture",
                   2: "Early Venture",
                   3: "Early Venture",
                   4: "Early Venture",
                   5: "Late Venture",
                   6: "Late Venture",
                   7: "Late Venture",
                   8: "Late Venture", # "Post Venture",
                   9: "Late Venture", # "Post Venture",
                   10: "Late Venture", # "Post Venture",                   
                   }


# company size categories
li_company_sizes = ['0-1','2-10', '11-50','51-200','201-500', '501-1,000','1001-5,000', '5001-10000','10001+']
cb_company_sizes = ['1-10','11-50', '51-100','101-250', '251-500', '1001-5000','501-1000', '5001-10000','10001+']

li_cb_size_map = {"51-100": "51-500", # CB category
               "101-250": "51-500", # CB category
               "251-500": "51-500", # CB category
               "51-200": "51-500", # LI category
               "201-500":"51-500",  # LI category
               "0-1": "1-10", # LI category
               "2-10": "1-10" # LI category
               }
size_commas = {"1000": "1,000", 
                   "1001": "1,001", 
                   "5000": "5,000",
                   "5001": "5,001", 
                   "10000":"10,000" ,
                   "10001": "10,001+"
                   }

gov_list = ['NASA',
            'USDA',
            'US Department of Energy',
            'Texas Commission on Environmental Quality',
            'National Institute of Standards and Technology', 
            'NYSERDA',
            'New Jersey Department of Environmental Protection',
            'New Jersey Economic Development Authority',
            'New Jersey Board of Public Utilities',
            'Commonwealth of Massachusetts',
            'U.S. Department of Energy Solar Energy Technologies Office',
            'UK Department for Transport','Kansas Department of Commerce',
            'Department for Business, Energy and Industrial Strategy',
            'Department for Promotion of Industry and Internal Trade (DPIIT)',
            'Michigan Department of Transportation', 
            'California Department of Food and Agriculture',
            'North Dakota Department of Agriculture', 
            'Minnesota Department of Agriculture',
            'California Department of Water Resources',
            'Oregon Department of Energy',
            'Michigan Department of Environment',
            'Great Lakes and Energy',
            'National Institutes of Health',
            'National Research Council of Canada Industrial Research Assistance Program',
            'ARPA-E',
            'NSF Small Business Innovation Research / Small Business Technology Transfer (SBIR/STTR)',
            'Nasa Small Business Innovation Research / Small Business Technology Transfer',
            'California Clean Energy Fund',
            'California Energy Commission',
            'Colorado Office of Economic Development and International Trade',
            'NASA',
            'U.S. Department of Energy Advanced Manufacturing Office',
            'U.S. Department of Energy Solar Energy Technologies Office',
            'Massachusetts Clean Energy Center',
            'National Institutes of Health',
            'National Institute of Standards and Technology',
            'Main Technology Institute',
            ]


not_gov_list = ['The Omidyar Group']