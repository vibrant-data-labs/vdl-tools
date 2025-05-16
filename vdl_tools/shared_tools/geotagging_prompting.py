import json
from json import JSONDecodeError

from sqlalchemy.orm.session import Session

from vdl_tools.shared_tools.openai.openai_api_utils import get_completion
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL
from vdl_tools.shared_tools.tools.logger import logger


def parse_results_to_dict(response):
    try:
        cleaned_text = response['response_text'].strip("```json").strip("```").strip()
        # sometimes json is malformed with a trailing comma
        cleaned_text = cleaned_text.replace('"country": null,\n', '"country": null\n')
        response_locations = json.loads(cleaned_text)
    except JSONDecodeError as er:
        logger.error(er)
        logger.error(response)
        return {}
    return response_locations


def get_organization_locations_from_response(response):
    response_locations = parse_results_to_dict(response)
    organization_locations = []
    # See where this was a {"metadatas": None} so if so, make sure it will be a list
    location_metadatas = response_locations.get('metadatas', []) or []
    for location_metadata in location_metadatas:
        location = location_metadata.get("entity")
        if location:
            organization_locations.append(location)
    return organization_locations


def geotag_texts_bulk(
    ids_texts: list[tuple],
    session: Session,
    max_workers: int=8,
    use_cached_result: bool=True,
):
    """Runs Geotagging on a set of texts. Uses cached results when set.

    The Geotagging is a done with a long formed prompt that was "tuned" prompt using DsPy.
    https://github.com/vibrant-data-labs/zein-hack-box/blob/main/notebooks/dspy_location_extraction.ipynb

    It is 

    Parameters
    ----------
    ids_texts : list[tuple]
        A tuple of (ids, text) used for geotagging and database retrieval
    session : Session
        SQLAlchemy session
    use_cached_results : bool, optional
        Whether to use cached results for previously run results, by default True

    Returns
    -------
    dict
        A dictionary with the id: list of dictionaries that each have the form:
        {id: list[dict]}
            {"city": str, "state_region": str, "country": str}
    """
    geotagger = GeoTaggingPromptCache(
        session=session
    )
    ids_to_response = geotagger.bulk_get_cache_or_run(
        given_ids_texts=ids_texts,
        session=session,
        use_cached_result=use_cached_result,
        max_workers=max_workers,
    )

    id_to_locations = {}

    for id_, response in ids_to_response.items():
        organization_locations = get_organization_locations_from_response(response)
        id_to_locations[id_] = organization_locations

    return id_to_locations


class GeoTaggingPromptCache(PromptResponseCacheSQL):
    def __init__(
        self,
        session,
    ):
        # If None or "" is passed in
        prompt_str = BASE_TEXT
        prompt_name = "geo_tagging_v1"
        super().__init__(
            session=session,
            prompt_str=prompt_str,
            prompt_name=prompt_name,
        )

    def get_completion(self, prompt_str, text, model="gpt-4.1-mini", **kwargs):
        """Writes a custom get completion with ignores a system prompt and focuses on the text prompt.
        
        This was built using the optomized prompting through DsPy work
        """

        full_text = f"""{self.prompt.prompt_str}
        ---

        Text: {text}
        Metadatas:
        """
        return get_completion(
            prompt=None,
            text=full_text,
            model=model,
            return_all=True,
            temperature=0,
            max_tokens=4096,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0,
        )


BASE_TEXT = """
You are a research assistant. Your task is to extract the locations, the value of the location and the reasoning for why the extracted value is the correct value.
Once extracted, you should add the most likely region and country if it doesn't exist.
Sometimes the location is a country. In this case, don't add city or state_region.
Sometimes the location is a state_region. In this case, don't add city.
If the location is a city, please try to add the most likely state_region and country. However, if you don't know the state_region, add null.
If the location is not in English, please translate it to English and return all results in English. If you cannot translate it, add null.
If you cannot extract the location, add null.

---

Follow the following format.

Text: ${text}
Metadatas: ${metadatas}. Respond with a single JSON object. JSON Schema: {"$defs": {"Location": {"properties": {"city": {"anyOf": [{"type": "string"}, {"type": "null"}], "description": "City name", "title": "City"}, "state_region": {"anyOf": [{"type": "string"}, {"type": "null"}], "description": "State or region name", "title": "State Region"}, "country": {"description": "Country name. Must be a valid country.", "title": "Country", "type": "string"}}, "required": ["city", "state_region", "country"], "title": "Location", "type": "object"}, "LocationMetaData": {"properties": {"reasoning": {"description": "Reasoning for why the location is correct", "title": "Reasoning", "type": "string"}, "entity": {"allOf": [{"$ref": "#/$defs/Location"}], "description": "The actual entity i.e. Paris, TX, USA; Beirut, Lebanon etc"}}, "required": ["reasoning", "entity"], "title": "LocationMetaData", "type": "object"}}, "properties": {"metadatas": {"items": {"$ref": "#/$defs/LocationMetaData"}, "title": "Metadatas", "type": "array"}}, "required": ["metadatas"], "title": "LocationMetaDatas", "type": "object"}

---

Text:
RYDE is a social enterprise that promotes carpooling. RYDE provides a sustainable alternative for the daily commute to solve the congestion and pollution issues facing cities today. // Sectors: Transportation; Mobile Apps; Software; Internet//
Ryde is a Singapore-based organization that offers a range of transportation services including carpooling, ride-hailing, and delivery. They aim to transform the way people and goods move around, fostering closer communities and promoting sustainable commuting. Ryde provides private-hire services, on-demand delivery, and opportunities for individuals to share rides and reduce their environmental impact. The organization emphasizes safety, high standards, and community building, with features such as comprehensive review systems and driver community standards. Ryde also offers various ride options, exclusive discounts, and a 0% commission rate for drivers. They focus on building a supportive and incentivized community, with a vision of carpooling as a key solution for urban congestion and environmental concerns.//
At Ryde, we believe in sustainable travelling. We’re here to make the daily commute better for all, while promoting a green and car-lite society. Being a technology company with mobility at the core of our business, we reimagine the way we move around - where communities are brought closer and travelling made sustainable.

Ryde started as a ride-sharing platform that matched drivers with riders going the same way to share a ride (RydePool), but we haven’t stopped there. We have established extended product services to include an e-payment platform (RydePay), taxi booking and insurance purchase. We continue to build features that benefit everyone.

Founded in September 2014, Ryde is currently available in Singapore and have presence in Malaysia, Hong Kong and Australia.

If you are an innovative individual with a passion for problem-solving and want to work where you are inspired to explore your curiosity, nurture your talents and make an impact, drop us a message!
Metadatas:
```json
{
  "metadatas": [
    {
      "reasoning": "Ryde is explicitly described as a Singapore-based organization, indicating its primary location and headquarters.",
      "entity": {
        "city": null,
        "state_region": null,
        "country": "Singapore"
      }
    },
    {
      "reasoning": "The text mentions Ryde's presence in Malaysia, Hong Kong, and Australia, indicating additional locations where Ryde operates.",
      "entity": {
        "city": null,
        "state_region": null,
        "country": "Malaysia"
      }
    },
    {
      "reasoning": "The text mentions Ryde's presence in Malaysia, Hong Kong, and Australia, indicating additional locations where Ryde operates.",
      "entity": {
        "city": null,
        "state_region": null,
        "country": "Hong Kong"
      }
    },
    {
      "reasoning": "The text mentions Ryde's presence in Malaysia, Hong Kong, and Australia, indicating additional locations where Ryde operates.",
      "entity": {
        "city": null,
        "state_region": null,
        "country": "Australia"
      }
    }
  ]
}
```

---

Text: We are a Texas based company with offices in Arizona as well. We have our farms located in Paris where the sun shines almost all day. We also have locations in Arlington, Dallas, and Phoenix.
Metadatas:
```json
{
  "metadatas": [
    {
      "reasoning": "Texas is mentioned as the base for the company, indicating it is a significant location for the company's operations.",
      "entity": {
        "city": null,
        "state_region": "Texas",
        "country": "USA"
      }
    },
    {
      "reasoning": "Arizona is mentioned as having offices of the company, making it a relevant location for the company's presence.",
      "entity": {
        "city": null,
        "state_region": "Arizona",
        "country": "USA"
      }
    },
    {
      "reasoning": "Paris is mentioned in the context of having farms, without specifying the country, it's assumed to be Paris, Texas due to the context of other US locations.",
      "entity": {
        "city": "Paris",
        "state_region": "Texas",
        "country": "USA"
      }
    },
    {
      "reasoning": "Arlington is mentioned as a location, and given the context of other Texas locations, it is assumed to be Arlington, Texas.",
      "entity": {
        "city": "Arlington",
        "state_region": "Texas",
        "country": "USA"
      }
    },
    {
      "reasoning": "Dallas is mentioned as a location, and given the context, it is assumed to be Dallas, Texas.",
      "entity": {
        "city": "Dallas",
        "state_region": "Texas",
        "country": "USA"
      }
    },
    {
      "reasoning": "Phoenix is mentioned as a location, and given the context of Arizona, it is identified as Phoenix, Arizona.",
      "entity": {
        "city": "Phoenix",
        "state_region": "Arizona",
        "country": "USA"
      }
    }
  ]
}
```

---

Text: Our solar farms in California are amazing but the ones in Arlington are even better
Metadatas:
```json
{
  "metadatas": [
    {
      "reasoning": "California is mentioned as a location with solar farms, indicating it's a state in the U.S.",
      "entity": {
        "city": null,
        "state_region": "California",
        "country": "USA"
      }
    },
    {
      "reasoning": "Arlington is mentioned as having better solar farms, implying it's a city. Given the context, the most likely Arlington is in Virginia, USA.",
      "entity": {
        "city": "Arlington",
        "state_region": "Virginia",
        "country": "USA"
      }
    }
  ]
}
```

---

Text: The EVERY Company is the market leader in engineering, manufacturing and formulating animal-free, animal proteins as ingredients for the global food and beverage industry. Leveraging advanced fermentation to produce functional and nutritional proteins from microorganisms, the Company is reimagining the factory farm model that underpins industrial animal agriculture with a more sustainable, kinder, and healthier alternative. Clara Foods is on a mission to decouple the world's most powerful proteins from the animals that make them. // Sectors: Sustainability; Food and Beverage; Biotechnology; Food Processing; Nutrition; Manufacturing\nThe Every Company is dedicated to creating animal-free proteins for a sustainable and ethical approach to protein production. They aim to revolutionize the food and beverage industry by providing proteins that meet high standards in taste, texture, and functionality without using any animal products. Through precision fermentation technology, they produce real animal proteins without compromising on quality, paving the way for a future where everyone can thrive while making ethical food choices. The company collaborates with various partners to distribute animal-free protein globally and has received investments to support its mission of transforming the food system.\nThe EVERY Company combines industry-leading expertise in food technology and flavor to enable the foods of tomorrow, today, for the world’s global food and beverage industries.\n\nWe’re creating game-changing solutions for businesses and brands, with our people and products delivering leveled-up performance, unprecedented outcomes in taste and texture, and animal-free innovation to a host of categories, in line with consumer trends.\n\nAlong the way, we’re co-creating a better tomorrow, commercializing nutritious, nature-equivalent animal proteins from fermentation that taste and function like the originals, while using significantly less land, water and greenhouse gas emissions than conventional animal proteins.\n\nNow in market, our versatile proteins are proven solutions for a variety of applications, from bars and beverages to bakery, alt meat and novel foods and beverages that benefit from a protein boost.\n\nAlongside Co-Founder and CEO Arturo Elizondo, we’re charting a course to a more resilient and transformative tomorrow, decoupling the world’s proteins from the animals that make them to power the next phase of food systems resilience – and rise to the challenge of feeding the next billion.
Metadatas:
```json
{
  "metadatas": [
    {
      "reasoning": "The text does not explicitly mention a specific location for The EVERY Company, so the location is null.",
      "entity": {
        "city": null,
        "state_region": null,
        "country": null,
      }
    }
  ]
}
```
"""
