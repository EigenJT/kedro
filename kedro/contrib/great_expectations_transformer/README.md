# (WIP) Great Expectations Transformer

So you've got an expectations_suite that you made using `great_expectations` and you would like to seamlessly integrate it into a `kedro` pipeline you've created? Do you want to use those great
data docs that `great_expectations` has made? Then you've come to the right place! Introducing the **Great Expectations Transformer**(patent pending). This transformer does those things, mostly without issues.

## Requirements

- python3.7
- kedro
- great_expectations

To install the requirements, run

```shell
pip install -r requirements.txt
```

## General Idea

This transformer aims to make it really easy to evaluate an expectations suite whenever a dataset in the `kedro` `catalog.yml` is loaded or saved. All you have to do is stuff the expectations somewhere in your `kedro` project (I like putting them in `data/00_expectations/`).
Then add the following to your `catalog.yml`:

```yaml
some_data_set:
    type: CSVLocalDataSet #(or something that kedro uses pandas to load)
    ...
    expectations_suite:
        filepath: path/to/expectations_suite.json
        break_on_failure: True
```

and voila, that expectation suite will be validated against the data in `some_data_set` whenever `kedro` loads or saves it. Then you can run the `great_expectations build-docs` command in the command line and see the lovely results of validations and such.

Easy, right? Well there's a little more to do.

## Set-up

### Step 1: Clone the repository

TODO: Can this be less ambigious about what the outcome is.  E.g. what the output file tree should look like.
TODO: Is git clone necessary? (assumes that later changes to this code will be made and re pulled) or perhaps just download a folder called ge_transformer with the three files?
TODO: Also `git clone` will create a directory with the name of the repo, but instructions specify just the contents of the repo?

Clone the contents of this repository into your kedro project into the `src/[project_name]/transformers/` folder. Create it if it doesn't exist.

```shell
git clone yada yada
```

#### Step 2: Modify the 'run.py' file

In order to use this transformer, the `kedro` `ProjectContext` class needs to be modified. This is in line with how transformers are used in [general](https://kedro.readthedocs.io/en/latest/04_user_guide/04_data_catalog.html#transforming-datasets). In the `src/project_name/run.py` file add the following to the imports

```python
...
from [project_name].transformers.expectations_evaluator import ExpectationsSetup, ExpectationsEvaluator, get_names_and_expectations_suite,remove_expectations_suite_from_catalog
```

modify the `ProjectContext` class with the following:

TODO: This could be explained in a better way. Ideally with a before/after picture about what exactly should be added
```python

# New dependencies to add Type checking support
from kedro.io import DataCatalog
from kedro.versioning import Journal

...

class ProjectContext(KedroContext):
    """Users can override the remaining methods from the parent class here,
    or create new ones (e.g. as required by plugins)
    """

    project_name = "ge_transformer"
    project_version = "0.15.5"

    def _get_catalog(
        self,
        save_version: str = None,
        journal: Journal = None,
        load_versions: Dict[str, str] = None,
    ) -> DataCatalog:
        """A hook for changing the creation of a DataCatalog instance.

        Returns:
            DataCatalog defined in `catalog.yml`.

        """
        #---------Needed for transformer----------#
        ExpectationsSetup()
        names_and_suites = get_names_and_expectations_suite(self)# get the names of the datasets with expectations_suite
        conf_catalog = remove_expectations_suite_from_catalog(self)# remove the expectations_suite from catalog
  
        #---------Not Needed for transformer (but don't remove)----------#
        conf_creds = self._get_config_credentials()
        catalog = self._create_catalog(
            conf_catalog, conf_creds, save_version, journal, load_versions
        )
        catalog.add_feed_dict(self._get_feed_dict())


        #---------Needed for transformer----------#
        for dataset in names_and_suites.keys():#add a specific ExpectationsEvaluator to each dataset
            catalog.add_transformer(ExpectationsEvaluator(dataset,expectations_suite = names_and_suites[dataset]["filepath"],break_on_failure = names_and_suites[dataset]["break_on_failure"],run_id = save_version),dataset)
        return catalog
```

and that's it.

### TODO: Add section about actually building the great_expectations docs. Long story short, run `great_expectations build-docs`



#### Restrictions

- Right now, this transformer only handles datasets that `kedro` implements using `pandas`. See [pandas](https://github.com/quantumblacklabs/kedro/tree/develop/kedro/extras/datasets) folder for an ongoing list of supported filetypes.
To extend it to everything, a mapping from `kedro` [datatype]DataSet to the equivalent in `great_expectations` would be required. I think kedro is looking to [remove](https://github.com/quantumblacklabs/kedro/issues/182#issuecomment-564315583) `pandas` as a dependency in the future,
so this transformer may have to be updated accordingly when that occurs.
Thinking `Apache Arrow` could be used for this.

#### Roadmap

- [ ] Address the restrictions
