from pydantic import BaseModel, Field

from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.database_cache.database_utils import get_session


class SearchTerm(BaseModel):
    term: str = Field(description="The search term")
    chain_of_thought: str = Field(description="The chain of thought process for the search term")
    # confidence: float = Field(description="The confidence that the new term is related to the input search term")

class SearchTermExpansionResponse(BaseModel):
    expanded_search_terms: list[SearchTerm]



PROMPT_TEMPLATE = """
You are a helpful assistant that expands search terms into a list of related search terms.
I'm going to give you a search term and you will return a list of related search terms.

Please bias towards terms that are more general and less specific as I'd like to have a broad range of results.

The broader topic should be related to: **%s**

Here are some examples:

Search term: "artificial intelligence"
Related search terms: [
 {
    "term": "machine learning",
    "chain_of_thought": "Machine learning is a subset of artificial intelligence. It is a type of machine learning that uses data to learn patterns and make predictions.",
    "confidence": 0.9
 },
 {
    "term": "deep learning",
    "chain_of_thought": "Deep learning is a type of machine learning that uses neural networks to learn patterns and make predictions.",
    "confidence": 0.8
 }
]

Search term: "climate change"
Related search terms: [
 {
    "term": "global warming",
    "chain_of_thought": "Global warming is a result of climate change.",
    "confidence": 0.9
 },
 {
    "term": "carbon emissions",
    "chain_of_thought": "Carbon emissions are a major contributor to climate change.",
    "confidence": 0.8
 },
 {
    "term": "sustainability",
    "chain_of_thought": "Sustainability is a key component of climate change.",
    "confidence": 0.87
 }
]

Search term: "kidney disease"
Related search terms: [
 {
    "term": "kidney failure",
    "chain_of_thought": "Kidney failure is a result of kidney disease.",
    "confidence": 0.9
 },
 {
    "term": "kidney transplant",
    "chain_of_thought": "Kidney transplant is a treatment for kidney disease.",
    "confidence": 0.85
 },
 {
    "term": "kidney treatment",
    "chain_of_thought": "Kidney treatment is a treatment for kidney disease.",
    "confidence": 0.89
 }
]
"""


class SearchTermExpansion(InstructorPRC):
    def __init__(
        self,
        session,
        topic,
        prompt_str=None,
        prompt_id=None,
        response_model=SearchTermExpansionResponse,
        prompt_name="search_term_expansion",
        model="gpt-4.1-mini",
    ):
        if not prompt_str:
            prompt_str = PROMPT_TEMPLATE % topic
        super().__init__(
            session=session,
            prompt_str=prompt_str,
            response_model=response_model,
            prompt_name=prompt_name,
            model=model,
            prompt_id=prompt_id,
        )
        self.topic = topic

    def expand_search_terms(
        self,
        search_term,
        use_cached_result=True,
        max_tokens=10000,
        previous_terms=None,
        **kwargs
    ):
        """Expands a search term into related search terms using the OpenAI API.

        Parameters
        ----------
        search_term : str
            The search term to expand
        use_cached_result : bool, optional
            Whether to use cached results, by default True
        max_tokens : int, optional
            Maximum tokens for the API response, by default 10000
        previous_terms : list[str], optional
            List of previous terms to ignore, by default None
        **kwargs
            Additional arguments passed to get_cache_or_run()

        Returns
        -------
        list[SearchTerm]
            List of expanded search terms, each containing:
            - term: The expanded search term
            - chain_of_thought: Explanation of relationship to original term  
            - confidence: Float indicating confidence score

        Notes
        -----
        Returns empty list if API call fails or no results found
        """
        text = f"Search term: {search_term}"
        if previous_terms:
            text += f"\nPrevious search terms to ignore: {previous_terms}"

        try:
            response = self.get_cache_or_run(
                given_id=f"{self.topic} - {search_term}",
                text=text,
                model=self.model,
                max_tokens=max_tokens,
                use_cached_result=use_cached_result,
                **kwargs
            )
        except Exception as e:
            logger.error(f"Error expanding search term {search_term}: {e}")
            return []
        if not response:
            return []
        return self.response_model.model_validate_json(response['response_text']).dict()['expanded_search_terms']


def expand_search_term_round(
    topic,
    search_term,
    use_cached_result=True,
    max_tokens=10000,
    model="gpt-4.1-mini",
    previous_terms=None,
    **kwargs
):
    with get_session() as session:
        search_term_expansion = SearchTermExpansion(session, topic, model=model)
        return search_term_expansion.expand_search_terms(
            search_term,
            use_cached_result,
            max_tokens,
            previous_terms,
            **kwargs
        )


def expand_search_term(
    topic,
    search_term,
    prompt_id=None,
    prompt_str=None,
    use_cached_result=True,
    max_tokens=10000,
    model="gpt-4.1-mini",
    max_rounds=3,
    stopping_threshold=.1,
    **kwargs
):
    """Expand a search term into related terms using an LLM.

    Parameters
    ----------
    topic : str
        The overall topic/domain for the search term expansion
    search_term : str
        The initial search term to expand
    prompt_str : str, optional
        The prompt to use for the search term expansion, by default None
    use_cached_result : bool, optional
        Whether to use cached results if available, by default True
    max_tokens : int, optional
        Maximum tokens for LLM response, by default 10000
    model : str, optional
        LLM model to use, by default "gpt-4.1-mini"
    max_rounds : int, optional
        Maximum number of expansion rounds, by default 3
    stopping_threshold : float, optional
        Stop when ratio of new terms to total terms falls below this, by default 0.1
    **kwargs
        Additional keyword arguments passed to expand_search_terms

    Returns
    -------
    list
        List of dictionaries containing expanded terms, each with:
        - term: str, the expanded search term
        - chain_of_thought: str, explanation of relationship to original term
        - confidence: float, confidence score for the expansion

    Notes
    -----
    The function stops expanding terms when either:
    - No new terms are found in a round
    - Maximum number of rounds is reached
    - Ratio of new terms falls below stopping_threshold
    """
    with get_session() as session:
        search_term_expansion = SearchTermExpansion(
            session,
            topic,
            prompt_str=prompt_str,
            prompt_id=prompt_id,
            model=model
        )

        n_rounds = 0
        total_terms = []
        continue_loop = True
        while continue_loop:
            logger.info("Expanding search term %s in round %d for topic %s", search_term, n_rounds, topic)
            previous_terms_words = [x['term'] for x in total_terms]
            terms = search_term_expansion.expand_search_terms(
                search_term,
                use_cached_result,
                max_tokens,
                previous_terms_words,
                **kwargs
            )
            total_terms.extend(terms)
            n_rounds += 1
            new_terms_words = [x['term'] for x in terms]
            new_terms_words_set = set(new_terms_words).difference(previous_terms_words)

            if len(new_terms_words) == 0:
                logger.info("No new terms found in round %d for topic %s", n_rounds, topic)
                continue_loop = False
            elif n_rounds >= max_rounds:
                logger.info("Max rounds reached for topic %s", topic)
                continue_loop = False
            # stop if the number of new terms is less than the stopping threshold
            elif len(new_terms_words_set) / len(new_terms_words) < stopping_threshold:
                logger.info("Stopping threshold reached for topic %s", topic)
                continue_loop = False

    return total_terms

  
if __name__ == "__main__":
    from vdl_tools.shared_tools.database_cache.database_utils import get_session

    # How it works with the default prompt
    terms = expand_search_term(
        topic="climate change and health",
        search_term="asthma",
        use_cached_result=True,
        max_rounds=20,
        stopping_threshold=.1,
        model="gpt-4.1",
        temperature=0.5,
    )


    # How it works with a specific prompt
    terms = expand_search_term(
        prompt_str="Give me related terms to the search term you will see.",
        topic="climate change and health",
        search_term="asthma",
        use_cached_result=True,
        max_rounds=20,
        stopping_threshold=.1,
        model="gpt-4.1",
        temperature=0.5,
    )
