#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 12 14:54:17 2021

@author: ericberlow
"""


# Government funder name lists (used by the coinvestor / investor prep scripts).
# The funding-type dicts that used to live here (fundingDict, stageDict,
# stageCategoriesDict) had no consumers and were replaced by
# vdl_tools.shared_tools.cb_funding_types; the unused company-size maps were
# removed at the same time.

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