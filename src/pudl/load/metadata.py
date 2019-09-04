"""Make me metadata!!!."""

import datetime
import hashlib
import importlib
import json
import logging
import os
import pathlib
import re
import shutil
import uuid

import datapackage
import goodtables

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)

##############################################################################
# CREATING PACKAGES AND METADATA
##############################################################################


def hash_csv(csv_path):
    """Calculates a SHA-i256 hash of the CSV file for data integrity checking.

    Args:
        csv_path (path-like) : Path the CSV file to hash.

    Returns:
        str: the hexdigest of the hash, with a 'sha256:' prefix.

    """
    # how big of a bit should I take?
    blocksize = 65536
    # sha256 is the fastest relatively secure hashing algorith.
    hasher = hashlib.sha256()
    # opening the file and eat it for lunch
    with open(csv_path, 'rb') as afile:
        buf = afile.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(blocksize)

    # returns the hash
    return f"sha256:{hasher.hexdigest()}"


def compile_partitions(pkg_settings):
    """
    Pull out the partitions from data package settings.

    Args:
        pkg_settings (dict): a dictionary containing package settings
            containing top level elements of the data package JSON descriptor
            specific to the data package

    Returns:
        dict:

    """
    partitions = {}
    for dataset in pkg_settings['datasets']:
        for dataset_name in dataset:
            try:
                partitions.update(dataset[dataset_name]['partition'])
            except KeyError:
                pass
    return(partitions)


def get_unpartioned_tables(tables, pkg_settings):
    """
    Get the tables w/out the partitions.

    Because the partitioning key will always be the name of the table without
    whatever element the table is being partitioned by, we can assume the names
    of all of the un-partitioned tables to get a list of tables that is easier
    to work with.

    Args:
        tables (iterable): list of tables that are included in this datapackage.
        pkg_settings (dictionary):
    Returns:
        iterable: tables_unpartioned is a set of un-partitioned tables
    """
    partitions = compile_partitions(pkg_settings)
    tables_unpartioned = set()
    if partitions:
        for table in tables:
            for part in partitions.keys():
                if part in table:
                    tables_unpartioned.add(part)
                else:
                    tables_unpartioned.add(table)
        return tables_unpartioned
    else:
        return tables


def package_files_from_table(table, pkg_settings):
    """Determine which files should exist in a package cooresponding to a table.

    We want to convert the datapackage tables and any information about package
    partitioning into a list of expected files. For each table that is
    partitioned, we want to add the partitions to the end of the table name.

    """
    partitions = compile_partitions(pkg_settings)
    files = []
    for dataset in pkg_settings['datasets']:
        try:
            partitions[table]
        except KeyError:
            if table not in files:
                files.append(table)
            continue
        try:
            for dataset_name in dataset:
                if dataset[dataset_name]['partition']:
                    for part in dataset[dataset_name][partitions[table]]:
                        file = table + "_" + str(part)
                        files.append(file)
        except KeyError:
            pass
    return(files)


def get_repartitioned_tables(tables, partitions, pkg_settings):
    """
    Get the re-partitioned tables.

    Args:
        tables (list): a list of tables that are included in this data package.
        partitions (dict)
        pkg_settings (dict): a dictionary containing package settings
            containing top level elements of the data package JSON descriptor
            specific to the data package.
    Returns:
        list: list of tables including full groups of
    """
    flat_pkg_settings = pudl.etl_pkg.get_flattened_etl_parameters(
        [pkg_settings])
    tables_repartitioned = []
    for table in tables:
        if partitions:
            for part in partitions.keys():
                if part is table:
                    for part_separator in flat_pkg_settings[partitions[part]]:
                        tables_repartitioned.append(
                            table + "_" + str(part_separator))
                else:
                    tables_repartitioned.append(table)
    return tables_repartitioned


def test_file_consistency(tables, pkg_settings, pkg_dir):
    """
    Test the consistency of tables for packaging.

    The purpose of this function is to test that we have the correct list of
    tables. There are three different ways we could determine which tables are
    being dumped into packages: a list of the tables being generated through
    the ETL functions, the list of dependent tables and the list of CSVs in
    package directory.

    Currently, this function is supposed to be fed the ETL function tables
    which are tested against the CSVs present in the package directory.

    Args:
        pkg_name (string): the name of the data package.
        tables (list): a list of table names to be tested.
        pkg_dir (path-like): the directory in which to check the consistency
            of table files

    Raises:
        AssertionError: If the tables in the CSVs and the ETL tables are not
            exactly the same list of tables.

    Todo:
        Determine what to do with the dependent tables check.

    """
    pkg_name = pkg_settings['name']
    # remove the '.csv' or the '.csv.gz' from the file names
    file_tbls = [re.sub(r'(\.csv.*$)', '', x) for x in os.listdir(
        os.path.join(pkg_dir, 'data'))]
    # given list of table names and partitions, generate list of expected files
    pkg_files = tables
    # pkg_files = []
    # for table in tables:
    #    pkg_file = package_files_from_table(table, pkg_settings)
    #    pkg_files.extend(pkg_file)

    dependent_tbls = list(
        pudl.helpers.get_dependent_tables_from_list_pkg(tables))
    etl_tbls = tables

    dependent_tbls.sort()
    file_tbls.sort()
    pkg_files.sort()
    etl_tbls.sort()
    # TODO: determine what to do about the dependent_tbls... right now the
    # dependent tables include some glue tables for FERC in particular, but
    # we are imagining the glue tables will be in another data package...
    if (file_tbls == pkg_files):  # & (dependent_tbls == etl_tbls)):
        logger.info(f"Tables are consistent for {pkg_name} package")
    else:
        inconsistent_tbls = []
        for tbl in file_tbls:
            if tbl not in pkg_files:
                inconsistent_tbls.extend(tbl)
                raise AssertionError(f"{tbl} from CSVs not in ETL tables")

        # for tbl in dependent_tbls:
        #    if tbl not in etl_tbls:
        #        inconsistent_tbls.extend(tbl)
        #        raise AssertionError(
        #            f"{tbl} from forgien key relationships not in ETL tables")
        # this is here for now just in case the previous two asserts don't work..
        # we should probably just stick to one.
        raise AssertionError(
            f"Tables are inconsistent. "
            f"Missing tables include: {inconsistent_tbls}")


def get_tabular_data_resource(table_name, pkg_dir, partitions=False):
    """
    Create a Tabular Data Resource descriptor for a PUDL table.

    Based on the information in the database, and some additional metadata this
    function will generate a valid Tabular Data Resource descriptor, according
    to the Frictionless Data specification, which can be found here:
    https://frictionlessdata.io/specs/tabular-data-resource/

    Args:
        table_name (string): table name for which you want to generate a
            Tabular Data Resource descriptor
        pkg_dir (path-like): The location of the directory for this package.
            The data package directory will be a subdirectory in the
            `datapackage_dir` directory, with the name of the package as the
            name of the subdirectory.

    Returns:
        Tabular Data Resource descriptor: A JSON object containing key
        information about the selected table

    """
    # every time we want to generate the cems table, we want it compressed
    if 'epacems' in table_name:
        abs_path = pathlib.Path(pkg_dir, 'data', f'{table_name}.csv.gz')
    else:
        abs_path = pathlib.Path(pkg_dir, 'data', f'{table_name}.csv')

    # pull the skeleton of the descriptor from the megadata file
    descriptor = pudl.helpers.pull_resource_from_megadata(table_name)
    descriptor['path'] = str(abs_path.relative_to(abs_path.parent.parent))
    descriptor['bytes'] = abs_path.stat().st_size
    descriptor['hash'] = pudl.output.export.hash_csv(abs_path)
    descriptor['created'] = (datetime.datetime.utcnow().
                             replace(microsecond=0).isoformat() + 'Z')

    if partitions:
        for part in partitions.keys():
            if part in table_name:
                descriptor['group'] = part

    resource = datapackage.Resource(descriptor)
    if resource.valid:
        logger.debug(f"{table_name} is a valid resource")
    if not resource.valid:
        raise AssertionError(
            f"""
            Invalid tabular data resource: {resource.name}

            Errors:
            {resource.errors}
            """
        )

    return descriptor


def get_source_metadata(data_sources, pkg_settings):
    """Grab sources for metadata."""
    sources = []
    for src in data_sources:
        if src in pudl.constants.data_sources:
            src_meta = {"title": src,
                        "path": pc.base_data_urls[src]}
            for dataset_dict in pkg_settings['datasets']:
                for dataset in dataset_dict:
                    # because we have defined eia as a dataset, but 860 and 923
                    # are separate sources, either the dataset must be or be in
                    # the source
                    if dataset in src or dataset == src:
                        src_meta['parameters'] = dataset_dict[dataset]
            sources.append(src_meta)
    return sources


def get_autoincrement_columns(unpartitioned_tables):
    """Grab the autoincrement columns for pkg tables."""
    with importlib.resources.open_text('pudl.package_data.meta.datapackage',
                                       'datapackage.json') as md:
        metadata_mega = json.load(md)
    autoincrement = {}
    for table in unpartitioned_tables:
        try:
            autoincrement[table] = metadata_mega['autoincrement'][table]
        except KeyError:
            pass
    return autoincrement


def validate_save_pkg(pkg_descriptor, pkg_dir):
    """
    Validate a data package descriptor and save it to a json file.

    Args:
        pkg_descriptor (dict):
        pkg_dir (path-like):

    Returns:
        report

    """
    # Use that descriptor to instantiate a Package object
    data_pkg = datapackage.Package(pkg_descriptor)

    # Validate the data package descriptor before we go to
    if not data_pkg.valid:
        logger.warning(f"""
            Invalid tabular data package: {data_pkg.descriptor["name"]}
            Errors: {data_pkg.errors}""")

    # pkg_json is the datapackage.json that we ultimately output:
    pkg_json = os.path.join(pkg_dir, "datapackage.json")
    data_pkg.save(pkg_json)
    logger.info('Validating the datapackage..')
    # Validate the data within the package using goodtables:
    report = goodtables.validate(pkg_json, row_limit=1000)
    if not report['valid']:
        logger.warning("Datapackage validation failed.")
    else:
        logger.info('Congrats! You made a validated a datapackage.')
    return report


def generate_metadata(pkg_settings, tables, pkg_dir,
                      uuid_pkgs=str(uuid.uuid4())):
    """
    Generate metadata for package tables and validate package.

    The metadata for this package is compiled from the pkg_settings and from
    the "megadata", which is a json file containing the schema for all of the
    possible pudl tables. Given a set of tables, this function compiles
    metadata and validates the metadata and the package. This function assumes
    datapackage CSVs have already been generated.

    See Frictionless Data for the tabular data package specification:
    http://frictionlessdata.io/specs/tabular-data-package/

    Args:
        pkg_settings (dict): a dictionary containing package settings
            containing top level elements of the data package JSON descriptor
            specific to the data package including:
            * name: short package name e.g. pudl-eia923, ferc1-test, cems_pkg
            * title: One line human readable description.
            * description: A paragraph long description.
            * keywords: For search purposes.
        tables (list): a list of tables that are included in this data package.
        pkg_dir (path-like): The location of the directory for this package.
            The data package directory will be a subdirectory in the
            `datapackage_dir` directory, with the name of the package as the
            name of the subdirectory.
        uuid_pkgs:

    Todo:
        Return to (uuid_pkgs)

    Returns:
        datapackage.package.Package: a datapackage. See frictionlessdata specs.
        dict: a valition dictionary containing validity of package and any
        errors that were generated during packaing.

    """
    # Create a tabular data resource for each of the tables.
    resources = []
    partitions = compile_partitions(pkg_settings)
    for table in tables:
        resources.append(get_tabular_data_resource(table,
                                                   pkg_dir=pkg_dir,
                                                   partitions=partitions))

    unpartitioned_tables = get_unpartioned_tables(tables, pkg_settings)
    data_sources = pudl.helpers.data_sources_from_tables_pkg(
        unpartitioned_tables)
    autoincrement = get_autoincrement_columns(unpartitioned_tables)
    sources = get_source_metadata(data_sources, pkg_settings)

    contributors = set()
    for src in data_sources:
        for c in pudl.constants.contributors_by_source[src]:
            contributors.add(c)

    pkg_descriptor = {
        "name": pkg_settings["name"],
        "profile": "tabular-data-package",
        "title": pkg_settings["title"],
        "id": uuid_pkgs,
        "description": pkg_settings["description"],
        # "keywords": pkg_settings["keywords"],
        "homepage": "https://catalyst.coop/pudl/",
        "created": (datetime.datetime.utcnow().
                    replace(microsecond=0).isoformat() + 'Z'),
        "contributors": [pudl.constants.contributors[c] for c in contributors],
        "sources": sources,
        "licenses": [pudl.constants.licenses["cc-by-4.0"]],
        "autoincrement": autoincrement,
        "resources": resources,
    }

    report = validate_save_pkg(pkg_descriptor, pkg_dir)
    return report


def prep_pkg_bundle_directory(pudl_settings,
                              pkg_bundle_dir_name,
                              clobber=False):
    """
    Create (or delete and create) data package directory.

    Args:
        pudl_settings (dict) : a dictionary filled with settings that mostly
            describe paths to various resources and outputs.
        debug (bool): If True, return a dictionary with package names (keys)
            and a list with the data package metadata and report (values).
        pkg_bundle_dir_name (string): name of directory you want the bundle of
            data packages to live. If this is set to None, the name will be
            defaulted to be the pudl packge version.
    Returns:
        path-like

    """
    pkg_bundle_dir = os.path.join(
        pudl_settings['datapackage_dir'], pkg_bundle_dir_name)

    if os.path.exists(pkg_bundle_dir) and (clobber is False):
        raise AssertionError(
            f'{pkg_bundle_dir} already exists and clobber is set to {clobber}')
    elif os.path.exists(pkg_bundle_dir) and (clobber is True):
        shutil.rmtree(pkg_bundle_dir)
    os.mkdir(pkg_bundle_dir)
    return(pkg_bundle_dir)
