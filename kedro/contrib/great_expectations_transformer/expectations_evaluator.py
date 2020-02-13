import logging
from kedro.io import AbstractTransformer
from pathlib import Path
from kedro.context import load_context, KedroContext
from kedro.io import DataCatalog
from typing import Dict, Callable, Any, List
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.data_context import DataContext
from great_expectations.data_context.types.base import NormalizedDataAssetName
import simplejson
import shutil


def get_names_and_expectations_suite(project_context: KedroContext) -> Dict:
    """Searches the project context for datasets that have the expectations_suite parameter, returns dictionary of datasets and their expectation suites
    Args:
        project_context (KedroContext): KedroContext object
    """
    catalog = project_context.config_loader.get("catalog*", "catalog*/**")
    datasets = catalog.keys()
    suites = {
        name: catalog[name]["expectations_suite"]
        for name in datasets
        if "expectations_suite" in catalog[name].keys()
    }
    return suites


def remove_expectations_suite_from_catalog(project_context: KedroContext) -> Dict:
    """removes any "expectations_suites" key:value pairs from a data catalog
    Args:
        project_context (KedroContext): KedroContext object
    """
    catalog = project_context.config_loader.get("catalog*", "catalog*/**")
    catalog = {
        name: {
            key: val
            for key, val in catalog[name].items()
            if key != "expectations_suite"
        }
        for name in catalog.keys()
    }
    return catalog


class FailedValidationException(Exception):
    pass


class ExpectationsSetup:
    def __init__(self):
        self.create_ge_project()  # check if project exists
        self.setup_datasources()

    @property
    def _logger(self):
        return logging.getLogger(self.__class__.__name__)

    def create_ge_project(self) -> None:
        """
        Creates a great_expectations default project directory if great_expectations.yml doesn't exist
        """
        if Path.is_file(Path.cwd() / "great_expectations" / "great_expectations.yml"):
            pass
        else:
            self._logger.info("Building great_expectations directory...")
            DataContext.create(Path.cwd())

    @property
    def _ge_data_context(self):
        """
        Accesses the great_expectations DataContext
        """
        return DataContext(Path.cwd() / "great_expectations")

    def create_generators(self) -> Dict:
        """Create a generator for each top level directory in the 'data'
        """
        top_directory = Path.cwd() / "data"
        n_directories = [
            child for child in top_directory.iterdir() if child.is_dir() == True
        ]
        if len(n_directories) == 0:
            generators = {
                str(top_directory.parts[-1]): {
                    "class_name": "SubdirReaderGenerator",
                    "base_directory": str(top_directory.parts[-1]),
                    "reader_options": {"sep": None, "engine": "python"},
                }
            }
        if len(n_directories) > 0:
            generators = {
                str(child.parts[-1]): {
                    "class_name": "SubdirReaderGenerator",
                    "base_directory": str(Path("data") / child.parts[-1]),
                    "reader_options": {"sep": None, "engine": "python"},
                }
                for child in top_directory.iterdir()
                if child.is_dir() == True
            }
        return generators

    def setup_datasources(self) -> None:
        context = self._ge_data_context
        generators = self.create_generators()
        context.datasources.clear()
        context.add_datasource(
            name="data",
            class_name="PandasDatasource",
            data_asset_type={"class_name": "PandasDataset"},
            generators=generators,
        )
        context._save_project_config()


class ExpectationsEvaluator(AbstractTransformer):
    """
    A transformer that evaluates expectations suites on save/load.
    """

    def __init__(
        self,
        kedro_dataset_name: str,
        expectations_suite: str = "",
        run_id: str = "",
        break_on_failure: bool = True,
    ) -> None:
        """Initializes the transformer
        Args:
            expectations_suite (str, optional): Path to expectations suite json file.
        """
        super().__init__()  # call AbstractTransformer init method
        self._expectations_suite = (
            expectations_suite  # add on the expectations suite path
        )
        self._run_id = run_id
        self._kedro_dataset_name = kedro_dataset_name
        self._break_on_failure = break_on_failure

    @property
    def logger(self):
        return logging.getLogger(self.__class__.__name__)

    @property
    def expectations_suite(self):
        return self._expectations_suite

    @property
    def run_id(self):
        return self._run_id

    @property
    def ge_data_context(self):
        """Accesses the great_expectations DataContext
        """
        return DataContext(Path.cwd() / "great_expectations")

    @property
    def kedro_data_context(self):
        """Accesses the kedro context"""
        return load_context(Path.cwd())

    @property
    def kedro_dataset_path(self) -> Path:
        """Gets the sourcefile path of the kedro dataset
        
        Returns:
            Path:
        """
        path = self.kedro_data_context.catalog.datasets.__dict__[
            self.kedro_dataset_name
        ].__dict__["_filepath"]
        return path

    @property
    def kedro_dataset_name(self) -> str:
        """Returns the kedro dataset name
        
        Returns:
            str:
        """
        return self._kedro_dataset_name

    @property
    def break_on_failure(self) -> bool:
        """Accesses the "break_on_failure" property
        
        Returns:
            bool
        """
        return self._break_on_failure

    def raise_on_failure(self, validation: Dict) -> None:
        """How to handle the failure of a validation
        
        Args:
            validation (Dict): A dictionary resulting from the validation of a dataset using great_expectations expectation_suite
        
        Raises:
            FailedValidationException: Exception to raise if validation fails and break_on_failure is true
        """
        if not self.break_on_failure and not validation["success"]:
            self.logger.warning("{}".format(validation["statistics"]))

        if validation["success"]:
            self.logger.info("{}".format(validation["statistics"]))

        if self.break_on_failure and not validation["success"]:
            failed_validations = {
                validation_result["expectation_config"][
                    "expectation_type"
                ]: validation_result["expectation_config"]["kwargs"]["column"]
                for validation_result in validation["results"]
                if not validation_result["success"]
            }
            self.logger.error(
                "Following expectations failed: {}".format(failed_validations)
            )
            raise FailedValidationException

    def get_ge_datasources(self) -> List[str]:
        """Access the datasources for the great_expectations project
        
        Returns:
            list: list of datasources
        """
        return list(self.ge_data_context.datasources.keys())

    def normalize_data_asset(self, ge_data_asset: Path) -> NormalizedDataAssetName:
        context = self.ge_data_context
        potential_path = Path(ge_data_asset)
        normalized = context.normalize_data_asset_name(str(potential_path))
        return normalized

    def copy_expectations_to_great_expectations(
        self, path_to_expectations: str
    ) -> None:
        """
        Given a data_asset (name of a dataset in the kedro catalog) and the path to the expectations suite being evaluated, copy that expectations suite to the great_expectations "expectations" folder for viewing
        Args:
            path_to_expectations (str): expectations location
        """
        normalized_data_asset = self.normalize_data_asset(self.kedro_dataset_path)
        root_directory = (
            self.ge_data_context.root_directory
        )  # get the great expectations root directory from context
        # TODO: Perhaps https://docs.python.org/3/library/pathlib.html#pathlib.PurePath.name
        path_to_expectations = Path(self.expectations_suite)
        expectations_suite_filename = path_to_expectations.parts[-1]
        expectations_copy_path = (
            Path(root_directory)
            / "expectations"
            / normalized_data_asset.datasource
            / normalized_data_asset.generator
            / normalized_data_asset.generator_asset
        )
        Path.mkdir(
            expectations_copy_path, exist_ok=True, parents=True
        )  # make the directory if it doesn't already exists
        shutil.copy(
            str(path_to_expectations),
            str(expectations_copy_path / expectations_suite_filename),
        )

    def load(self, data_set_name: str, load: Callable[[], Any]) -> Any:
        data = load()
        expectations_data = PandasDataset(data)
        self.copy_expectations_to_great_expectations(
            self.expectations_suite
        )  # copy the expectations over to the great_expectations context
        self.logger.info(
            "validating: {} against {} ".format(
                self.kedro_dataset_name, Path(self.expectations_suite).parts[-1]
            )
        )
        validation = expectations_data.validate(
            expectation_suite=self.expectations_suite,
            run_id=self.run_id,
            catch_exceptions=True,
            result_format="COMPLETE",
        )
        self.save_validations(validation)
        self.raise_on_failure(validation)
        return data

    def save(self, data_set_name: str, save: Callable[[Any], None], data: Any) -> None:
        save(data)
        self.copy_expectations_to_great_expectations(
            self.expectations_suite
        )  # copy the expectations over to the great_expectations context
        expectations_data = PandasDataset(data)
        self.logger.info(
            "validating: {} against {} ".format(
                self.kedro_dataset_name, Path(self.expectations_suite).parts[-1]
            )
        )
        validation = expectations_data.validate(
            expectation_suite=self.expectations_suite,
            run_id=self.run_id,
            catch_exceptions=True,
            result_format="COMPLETE",
        )
        self.save_validations(validation)
        self.raise_on_failure(validation)

    def save_validations(self, validation: dict) -> None:
        """Save validations to proper locations
        Args:
            validation (dict): Description
            data_set_name (str): Description
        """
        root_directory = self.ge_data_context.root_directory
        data_set_path = self.kedro_dataset_path
        normalized_data_asset = self.normalize_data_asset(data_set_path)
        expectations_suite_name = validation["meta"]["expectation_suite_name"]
        validations_file_name = expectations_suite_name + ".json"
        validations_location = (
            Path(root_directory)
            / "uncommitted"
            / "validations"
            / self.run_id
            / normalized_data_asset.datasource
            / normalized_data_asset.generator
            / normalized_data_asset.generator_asset
        )
        Path.mkdir(validations_location, parents=True, exist_ok=True)
        with open(validations_location / validations_file_name, "w") as file:
            simplejson.dump(validation, file)
            file.close()
