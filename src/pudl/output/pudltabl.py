"""
This module provides a class enabling tabular compilations from the PUDL DB.

Many of our potential users are comfortable using spreadsheets, not databases,
so we are creating a collection of tabular outputs that contain the most
useful core information from the PUDL data packages, including additional keys
and human readable names for the objects (utilities, plants, generators) being
described in the table.

These tabular outputs can be joined with each other using those keys, and used
as a data source within Microsoft Excel, Access, R Studio, or other data
analysis packages that folks may be familiar with.  They aren't meant to
completely replicate all the data and relationships contained within the full
PUDL database, but should serve as a generally usable set of PUDL data
products.

The PudlTabl class can also provide access to complex derived values, like the
generator and plant level marginal cost of electricity (MCOE), which are
defined in the analysis module.

In the long run, this is a probably a kind of prototype for pre-packaged API
outputs or data products that we might want to be able to provide to users a la
carte.

Todo:
    Return to for update arg and returns values in functions below

"""

import logging
from inspect import signature
from pathlib import Path

# Useful high-level external modules.
import pandas as pd
import sqlalchemy as sa

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)


###############################################################################
#   Output Class, that can pull all the below tables with similar parameters
###############################################################################


class PudlTabl(object):
    """A class for compiling common useful tabular outputs from the PUDL DB."""

    # EIA plant-utility associations
    pu_eia860 = "pu_eia860"
    # FERC plant-utility associations.
    pu_ferc1 = "pu_ferc1"

    ###########################################################################
    # EIA 861 Interim Outputs (awaiting full DB integration)
    ###########################################################################
    """
    This is an interim solution that provides a (somewhat) standard way of accessing
    the EIA 861 data prior to its being fully integrated into the PUDL database. If
    any of the dataframes is attempted to be accessed, all of them are set. Only
    the tables that have actual transform functions are included, and as new
    transform functions are completed, they would need to be added to the list
    below. Surely there is a way to do this automatically / magically but that's
    beyond my knowledge right now.
    """
    advanced_metering_infrastructure_eia861 = "advanced_metering_infrastructure_eia861"
    balancing_authority_eia861 = "balancing_authority_eia861"
    balancing_authority_assn_eia861 = "balancing_authority_assn_eia861"
    demand_response_eia861 = "demand_response_eia861"
    demand_side_management_eia861 = "demand_side_management_eia861"
    distributed_generation_eia861 = "distributed_generation_eia861"
    distribution_systems_eia861 = "distribution_systems_eia861"
    dynamic_pricing_eia861 = "dynamic_pricing_eia861"
    energy_efficiency_eia861 = "energy_efficiency_eia861"
    green_pricing_eia861 = "green_pricing_eia861"
    mergers_eia861 = "mergers_eia861"
    net_metering_eia861 = "net_metering_eia861"
    non_net_metering_eia861 = "non_net_metering_eia861"
    operational_data_eia861 = "operational_data_eia861"
    reliability_eia861 = "reliability_eia861"
    sales_eia861 = "sales_eia861"
    service_territory_eia861 = "service_territory_eia861"
    utility_assn_eia861 = "utility_assn_eia861"
    utility_data_eia861 = "utility_data_eia861"
    ###########################################################################
    # End EIA 861 Interim Outputs
    ###########################################################################

    ###########################################################################
    # FERC 714 Interim Outputs (awaiting full DB integration)
    ###########################################################################
    """
    This is an interim solution, so that we can have a (relatively) standard way of
    accessing the FERC 714 data prior to getting it integrated into the PUDL DB.
    Some of these are not yet cleaned up, but there are dummy transform functions
    which pass through the raw DFs with some minor alterations, so all the data is
    available as it exists right now.

    An attempt to access *any* of the dataframes results in all of them being
    populated, since generating all of them is almost the same amount of work as
    generating one of them.
    """

    respondent_id_ferc714 = "respondent_id_ferc714"
    demand_hourly_pa_ferc714 = "demand_hourly_pa_ferc714"
    description_pa_ferc714 = "description_pa_ferc714"
    id_certification_ferc714 = "id_certification_ferc714"
    gen_plants_ba_ferc714 = "gen_plants_ba_ferc714"
    demand_monthly_ba_ferc714 = "demand_monthly_ba_ferc714"
    net_energy_load_ba_ferc714 = "net_energy_load_ba_ferc714"
    adjacency_ba_ferc714 = "adjacency_ba_ferc714"
    interchange_ba_ferc714 = "interchange_ba_ferc714"
    lambda_hourly_ba_ferc714 = "lambda_hourly_ba_ferc714"
    lambda_description_ferc714 = "lambda_description_ferc714"
    demand_forecast_pa_ferc714 = "demand_forecast_pa_ferc714"

    ###########################################################################
    # EIA 860/923 OUTPUTS
    ###########################################################################
    # Utilities reported in EIA 860.
    utils_eia860 = "utils_eia860"
    # Boiler-generator associations from EIA 860.
    bga_eia860 = "bga_eia860"
    # Plant level info reported in EIA 860
    plants_eia860 = "plants_eia860"
    # Generators as reported in EIA 860
    gens_eia860 = "gens_eia860"
    # Generator level ownership data from EIA 860
    own_eia860 = "own_eia860"
    # EIA 923 generation and fuel consumption data
    gf_eia923 = "gf_eia923"
    # EIA 923 fuel receipts and costs data
    frc_eia923 = "frc_eia923"
    # EIA 923 boiler fuel consumption data
    bf_eia923 = "bf_eia923"
    # EIA 923 net generation data by generator
    """
    Net generation is reported in two seperate tables in EIA 923: in the
    generation_eia923 and generation_fuel_eia923 tables. While the
    generation_fuel_eia923 table is more complete (the generation_eia923
    table includes only ~55% of the reported MWhs), the generation_eia923
    table is more granular (it is reported at the generator level).

    This method either grabs the generation_eia923 table that is reported
    by generator, or allocates net generation from the
    generation_fuel_eia923 table to the generator level.
    """
    gen_eia923 = "gen_eia923"
    # Original EIA 923 net generation data by generator
    gen_original_eia923 = "gen_original_eia923"
    # Net generation from gen fuel table allocated to generators
    gen_allocated_eia923 = "gen_allocated_eia923"

    ###########################################################################
    # FERC FORM 1 OUTPUTS
    ###########################################################################

    # FERC Form 1 steam plants data.
    plants_steam_ferc1 = "plants_steam_ferc1"
    # FERC Form 1 steam plants fuel consumption data
    fuel_ferc1 = "fuel_ferc1"
    # FERC Form 1 fuel usage by plant
    fbp_ferc1 = "fbp_ferc1"
    # FERC Form 1 Small Plants Table
    plants_small_ferc1 = "plants_small_ferc1"
    # FERC Form 1 Hydro Plants Table
    plants_hydro_ferc1 = "plants_hydro_ferc1"
    # FERC Form 1 Pumped Storage Table
    plants_pumped_storage_ferc1 = "plants_pumped_storage_ferc1"
    # FERC Form 1 Purchased Power Table
    purchased_power_ferc1 = "purchased_power_ferc1"
    # FERC Form 1 Plant in Service Table
    plant_in_service_ferc1 = "plant_in_service_ferc1"

    ###########################################################################
    # EIA MCOE OUTPUTS
    ###########################################################################

    # The more complete EIA/PUDL boiler-generator associations
    bga = "bga"
    # Generator level heat rates (mmBTU/MWh)
    hr_by_gen = "hr_by_gen"
    # Generation unit level heat rates
    hr_by_unit = "hr_by_unit"
    # Generator level fuel costs per MWh
    fuel_cost = "fuel_cost"
    # Generator level capacity factors
    capacity_factor = "capacity_factor"
    # Generator level MCOE based on EIA data
    """
    Eventually this calculation will include non-fuel operating expenses
    as reported in FERC Form 1, but for now only the fuel costs reported
    to EIA are included. They are attibuted based on the unit-level heat
    rates and fuel costs.

    Args:
        min_heat_rate: lowest plausible heat rate, in mmBTU/MWh. Any MCOE
            records with lower heat rates are presumed to be invalid, and
            are discarded before returning.
        min_cap_fact: minimum generator capacity factor. Generator records
            with a lower capacity factor will be filtered out before
            returning. This allows the user to exclude generators that
            aren't being used enough to have valid.
        min_fuel_cost_per_mwh: minimum fuel cost on a per MWh basis that is
            required for a generator record to be considered valid. For
            some reason there are now a large number of $0 fuel cost
            records, which previously would have been NaN.
        max_cap_fact: maximum generator capacity factor. Generator records
            with a lower capacity factor will be filtered out before
            returning. This allows the user to exclude generators that
            aren't being used enough to have valid.
    """
    mcoe = "mcoe"

    def __init__(
        self,
        pudl_engine,
        ds=None,
        freq=None,
        start_date=None,
        end_date=None,
        fill_fuel_cost=False,
        roll_fuel_cost=False,
        fill_net_gen=False,
    ):
        """
        Initialize the PUDL output object.

        Private data members are not initialized until they are requested.
        They are then cached within the object unless they get re-initialized
        via a method that includes update=True.

        Some methods (e.g mcoe) will take a while to run, since they need to
        pull substantial data and do a bunch of calculations.

        Args:
            freq (str): String describing time frequency at which to aggregate
                the reported data. E.g. 'MS' (monthly start).
            start_date (date): Beginning date for data to pull from the
                PUDL DB.
            end_date (date): End date for data to pull from the PUDL DB.
            pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
                for the PUDL DB.
            fill_fuel_cost (boolean): if True, fill in missing EIA fuel cost
                from ``frc_eia923()`` with state-level monthly averages from EIA's
                API.
            roll_fuel_cost (boolean): if True, apply a rolling average
                to a subset of output table's columns (currently only
                'fuel_cost_per_mmbtu' for the frc table).
            fill_net_gen (boolean): if True, use net generation from the
                generation_fuel_eia923 - which is reported at the
                plant/fuel/prime mover level - re-allocated to generators in
                ``mcoe()``, ``capacity_factor()`` and ``heat_rate_by_unit()``.

        """
        self.pudl_engine = pudl_engine
        self.freq = freq
        # We need datastore access because some data is not yet integrated into the
        # PUDL DB. See the etl_eia861 method.
        self.ds = ds
        if self.ds is None:
            pudl_in = Path(pudl.workspace.setup.get_defaults()["pudl_in"])
            self.ds = pudl.workspace.datastore.Datastore(
                local_cache_path=pudl_in / "data"
            )

        # grab all working eia dates to use to set start and end dates if they
        # are not set
        eia_dates = pudl.helpers.get_working_eia_dates()
        if start_date is None:
            self.start_date = min(eia_dates)

        else:
            # Make sure it's a date... and not a string.
            self.start_date = pd.to_datetime(start_date)

        if end_date is None:
            self.end_date = max(eia_dates)
        else:
            # Make sure it's a date... and not a string.
            self.end_date = pd.to_datetime(end_date)

        if not pudl_engine:
            raise AssertionError("PudlTabl object needs a pudl_engine")

        self.roll_fuel_cost = roll_fuel_cost
        self.fill_fuel_cost = fill_fuel_cost
        self.fill_net_gen = fill_net_gen

        self.data_functions = {
            PudlTabl.pu_eia860: lambda: pudl.output.eia860.plants_utils_eia860(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            ),
            PudlTabl.pu_ferc1: lambda: pudl.output.ferc1.plants_utils_ferc1(
                self.pudl_engine
            ),
            # Begin interim eia861 functions
            PudlTabl.advanced_metering_infrastructure_eia861: self._interim_eia861,
            PudlTabl.balancing_authority_eia861: self._interim_eia861,
            PudlTabl.balancing_authority_assn_eia861: self._interim_eia861,
            PudlTabl.demand_response_eia861: self._interim_eia861,
            PudlTabl.demand_side_management_eia861: self._interim_eia861,
            PudlTabl.distributed_generation_eia861: self._interim_eia861,
            PudlTabl.distribution_systems_eia861: self._interim_eia861,
            PudlTabl.dynamic_pricing_eia861: self._interim_eia861,
            PudlTabl.energy_efficiency_eia861: self._interim_eia861,
            PudlTabl.green_pricing_eia861: self._interim_eia861,
            PudlTabl.mergers_eia861: self._interim_eia861,
            PudlTabl.net_metering_eia861: self._interim_eia861,
            PudlTabl.non_net_metering_eia861: self._interim_eia861,
            PudlTabl.operational_data_eia861: self._interim_eia861,
            PudlTabl.reliability_eia861: self._interim_eia861,
            PudlTabl.sales_eia861: self._interim_eia861,
            PudlTabl.service_territory_eia861: self._interim_eia861,
            PudlTabl.utility_assn_eia861: self._interim_eia861,
            PudlTabl.utility_data_eia861: self._interim_eia861,

            # Begin interim ferc 714 functions
            PudlTabl.respondent_id_ferc714: self._interim_ferc714,
            PudlTabl.demand_hourly_pa_ferc714: self._interim_ferc714,
            PudlTabl.description_pa_ferc714: self._interim_ferc714,
            PudlTabl.id_certification_ferc714: self._interim_ferc714,
            PudlTabl.gen_plants_ba_ferc714: self._interim_ferc714,
            PudlTabl.demand_monthly_ba_ferc714: self._interim_ferc714,
            PudlTabl.net_energy_load_ba_ferc714: self._interim_ferc714,
            PudlTabl.adjacency_ba_ferc714: self._interim_ferc714,
            PudlTabl.interchange_ba_ferc714: self._interim_ferc714,
            PudlTabl.lambda_hourly_ba_ferc714: self._interim_ferc714,
            PudlTabl.lambda_description_ferc714: self._interim_ferc714,
            PudlTabl.demand_forecast_pa_ferc714: self._interim_ferc714,

            # Begin EIA 860/923 outputs
            PudlTabl.utils_eia860: lambda: pudl.output.eia860.utilities_eia860(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            ),
            PudlTabl.bga_eia860: lambda: pudl.output.eia860.boiler_generator_assn_eia860(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            ),
            PudlTabl.plants_eia860: lambda: pudl.output.eia860.plants_eia860(
                self.pudl_engine,
                start_date=self.start_date,
                end_date=self.end_date,
            ),
            PudlTabl.gens_eia860: lambda: pudl.output.eia860.generators_eia860(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            ),
            PudlTabl.own_eia860: lambda: pudl.output.eia860.ownership_eia860(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            ),
            PudlTabl.gf_eia923: lambda: pudl.output.eia923.generation_fuel_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date,
            ),
            PudlTabl.frc_eia923: lambda: pudl.output.eia923.fuel_receipts_costs_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date,
                fill=self.fill_fuel_cost,
                roll=self.roll_fuel_cost,
            ),
            PudlTabl.bf_eia923: lambda: pudl.output.eia923.boiler_fuel_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date,
            ),
            PudlTabl.gen_eia923: self.gen_eia923,
            PudlTabl.gen_original_eia923: pudl.output.eia923.generation_eia923(
                self.pudl_engine,
                freq=self.freq,
                start_date=self.start_date,
                end_date=self.end_date,
            ),
            PudlTabl.gen_allocated_eia923: pudl.analysis.allocate_net_gen.allocate_gen_fuel_by_gen(self),

            # Begin ferc form 1 outputs
            PudlTabl.plants_steam_ferc1: lambda: pudl.output.ferc1.plants_steam_ferc1(
                self.pudl_engine
            ),
            PudlTabl.fuel_ferc1: lambda: pudl.output.ferc1.fuel_ferc1(self.pudl_engine),
            PudlTabl.fbp_ferc1: lambda: pudl.output.ferc1.fuel_by_plant_ferc1(
                self.pudl_engine
            ),
            PudlTabl.plants_small_ferc1: lambda: pudl.output.ferc1.plants_small_ferc1(
                self.pudl_engine
            ),
            PudlTabl.plants_hydro_ferc1: lambda: pudl.output.ferc1.plants_hydro_ferc1(
                self.pudl_engine
            ),
            PudlTabl.plants_pumped_storage_ferc1: lambda: pudl.output.ferc1.plants_pumped_storage_ferc1(self.pudl_engine),
            PudlTabl.purchased_power_ferc1: lambda: pudl.output.ferc1.purchased_power_ferc1(self.pudl_engine),
            PudlTabl.plant_in_service_ferc1: lambda: pudl.output.ferc1.plant_in_service_ferc1(self.pudl_engine),

            # Begin EIA MCOE outputs
            PudlTabl.bga: lambda: pudl.output.glue.boiler_generator_assn(
                self.pudl_engine, start_date=self.start_date, end_date=self.end_date
            ),
            PudlTabl.hr_by_gen: lambda: pudl.analysis.mcoe.heat_rate_by_gen(self),
            PudlTabl.hr_by_unit: lambda: pudl.analysis.mcoe.heat_rate_by_unit(self),
            PudlTabl.fuel_cost: lambda: pudl.analysis.mcoe.fuel_cost(self),
            PudlTabl.capacity_factor: lambda update, min_cap_fact=None, max_cap_fact=None: pudl.analysis.mcoe.capacity_factor(
                self, min_cap_fact=min_cap_fact, max_cap_fact=max_cap_fact
            ),
            PudlTabl.mcoe: lambda update,
            min_heat_rate=5.5,
            min_fuel_cost_per_mwh=0.0,
            min_cap_fact=0.0,
            max_cap_fact=1.5:
            pudl.analysis.mcoe.mcoe(
                self,
                min_heat_rate=min_heat_rate,
                min_fuel_cost_per_mwh=min_fuel_cost_per_mwh,
                min_cap_fact=min_cap_fact,
                max_cap_fact=max_cap_fact,
            ),

        }

        # We populate this library of dataframes as they are generated, and
        # allow them to persist, in case they need to be used again.
        self._dfs = {}

    def get_data(self, name, update=False, **kwargs):
        """
        Pull a dataframe describing the requested data.

        Args:
            update (bool): If true, re-calculate the output dataframe, even if
                a cached version exists.

        Returns:
            pandas.DataFrame: a denormalized table for interactive use.

        """
        if name not in self.data_functions:
            raise Exception(
                f'Unknown data source "{name}" passed into the get_data function')
        if update or name not in self._dfs:
            function = self.data_functions[name]
            num_args = len(signature(function).parameters)
            if num_args == 1:
                # If the function requires update, we pass it in
                result = function(update)
            elif num_args > 1:
                # If the function takes in other arguments, we pass them in
                result = function(update, **kwargs)
            else:
                result = function()

            # We check if there is a result for the interim cases where lots of dfs are set and no single one is actually returned
            if result:
                self._dfs[name] = result
        return self._dfs.get(name)

    def _interim_eia861(self):
        """
        Interim eia861 function
        """
        logger.warning("Running the interim EIA 861 ETL process!")
        eia861_raw_dfs = pudl.extract.eia861.Extractor(self.ds).extract(
            year=pc.working_partitions["eia861"]["years"]
        )
        eia861_tfr_dfs = pudl.transform.eia861.transform(eia861_raw_dfs)
        for table in eia861_tfr_dfs:
            self._dfs[table] = eia861_tfr_dfs[table]

    def _interim_ferc714(self):
        """
        Interim ferc714 function

        """
        logger.warning("Running the interim FERC 714 ETL process!")

        ferc714_raw_dfs = pudl.extract.ferc714.extract(ds=self.ds)
        ferc714_tfr_dfs = pudl.transform.ferc714.transform(ferc714_raw_dfs)
        for table in ferc714_tfr_dfs:
            self._dfs[table] = ferc714_tfr_dfs[table]

    def _gen_eia923(self, update):
        """
        Function for generation eia92

        We've made it a function instead of lambda in order to add logging
        """
        if self.fill_net_gen:
            logger.info(
                "Allocating net generation from the generation_fuel_eia923 "
                "to the generator level instead of using the less complete "
                "generation_eia923 table."
            )
            return self.get_data(PudlTabl.gen_allocated_eia923, update=update)
        else:
            return self.get_data(PudlTabl.gen_original_eia923, update=update)


def get_table_meta(pudl_engine):
    """Grab the pudl sqlitie database table metadata."""
    md = sa.MetaData()
    md.reflect(pudl_engine)
    return md.tables
