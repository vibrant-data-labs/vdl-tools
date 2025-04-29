# Project Relevance

This are the functions that were used in the Elemental "Project Relevance" project in March/April 2024. Most of these files are still un-tested in a generalized form, however, we are moving here to make them available for future use.

## Files

- `get_founder_linkedins_1.py` - This file scrapes the LinkedIns for the founders from the Crunchbase pull and adds some minor annotations.
- `get_skills_keywords_2.py` - Only need to run this once. It goes through the skills from the files generated in `get_founder_linkedins_1.py` and uses GPT to categorize them into a hierarchy which is helpful for human review.
  - After generating the skills file, a human needs to review and add a column for `project_relevant` which is a boolean value of whether the term would indicate a founder with project relevant experience
- `add_categorical_skills_to_linkedin_3.py` - This file adds the categorical skills to the founders' LinkedIns.
- `add_founder_toplines_to_venture_cft_4.py` - Based on whether the founder was considered to have project relevant skills, aggregates to the company level.
- `sensitivity_calculation_loops_5.py` - Runs sensitivity analysis by changing the threshold and skills that are used in the files above (effectively replaces the need for `_3.py` and `_4.py`). Does not generate a single answer, hundreds or thousands of runs that can be aggergated to get a sense of the sensitivity of the model.