import marimo

__generated_with = "0.10.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    from vdl_tools.shared_tools.taxonomy_mapping.few_shot_cache import FewShotCache, EntityActivityDict, EntityActivityDictExamplesDict, ExampleDict, EXAMPLES_CLASSED

    from vdl_tools.shared_tools.database_cache.database_utils import Session
    return (
        EXAMPLES_CLASSED,
        EntityActivityDict,
        EntityActivityDictExamplesDict,
        ExampleDict,
        FewShotCache,
        Session,
        mo,
    )


@app.cell
def _(Session):
    session = Session()
    return (session,)


@app.cell
def _(FewShotCache, session):
    cache = FewShotCache(
        session=session,
        model='gpt-4.1-mini',
    )
    return (cache,)


@app.cell
def _(EntityActivityDict, EntityActivityDictExamplesDict, ExampleDict):
    my_test = EntityActivityDictExamplesDict(
        entity_activity_dict=EntityActivityDict(
            entity_description="A company that works on building solar panels",
            activity_description="Watering plants helps plants grow.",
            activity_name="watering plant",
        ),
        examples_dicts=[
            ExampleDict(
                entity_description="Acme co builds nuclear power plants",
                activity_description="Gardening is a leisure activity",
                relevant=False,
                activity_name="gardening",
            )
        ]
    )
    return (my_test,)


@app.cell
def _(EntityActivityDict):
    entity_activity_dict = EntityActivityDict(
        entity_description="A company that works on building solar panels",
        activity_description="Watering plants helps plants grow.",
        activity_name="watering plant",
    )
    return (entity_activity_dict,)


@app.cell
def _(EXAMPLES_CLASSED):
    EXAMPLES_CLASSED
    return


@app.cell
def _(EXAMPLES_CLASSED, cache, entity_activity_dict):
    x = cache._format_messages(
        examples_dicts=[x.dict() for x in EXAMPLES_CLASSED],
        entity_activity_dict=entity_activity_dict.dict()
    )
    return (x,)


@app.cell
def _(x):
    print(x[1]['content'])
    return


@app.cell
def _(cache, my_test):
    cache.get_completion(None, my_test)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
