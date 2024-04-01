package com.vsb.baca;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.csv.CsvRules;
import org.apache.calcite.adapter.csv.CsvScannableTable;
import org.apache.calcite.adapter.csv.CsvSchemaFactory;
import org.apache.calcite.adapter.csv.CsvStreamTableFactory;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.file.CsvProjectTableScanRule;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.adapter.file.CsvTranslatableTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Source;
import org.apache.log4j.BasicConfigurator;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Basic calcite example
 *
 */
public class Main 
{
    public static void main( String[] args ) throws SqlParseException {
        BasicConfigurator.configure();

//        TODO 1 - zprovoznit SQL nad nejakymi csv daty (ClickBench?)

        // Initialize schema
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
//        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

        // Define table schema
        builder.add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER));
        builder.add("Name", typeFactory.createSqlType(SqlTypeName.VARCHAR, 20));
        builder.add("Salary", typeFactory.createSqlType(SqlTypeName.INTEGER));
        String fileName = "person.csv";
        final File base = new File("src/main/resources");
        final Source source = Sources.file(base, fileName);
        final RelProtoDataType protoRowType = RelDataTypeImpl.proto(builder.build());
        schema.add("Person", new CsvScannableTable(source, protoRowType));

        // SQL parsing
        SqlParser parser = SqlParser.create("SELECT salary, name FROM Person");
        // Parse the query into an AST
        SqlNode sqlNode = parser.parseQuery();
        System.out.println("[Parsed query]");
        System.out.println(sqlNode.toString());

        // Configure and instantiate validator
        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
                Collections.singletonList("bs"),
                typeFactory, config);

        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT);

        // Validate the initial AST
        SqlNode validNode = validator.validate(sqlNode);

        // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
        RelOptCluster cluster = newCluster(typeFactory);
        SqlToRelConverter relConverter = new SqlToRelConverter(
                NOOP_EXPANDER,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());

        // Convert the valid AST into a logical plan
        RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

        // Display the logical plan
        System.out.println(
                RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.NON_COST_ATTRIBUTES));
//        TODO 3 - seznamit se vice s operandy RelNode a semantikou jejich vypisu



        // Initialize optimizer/planner with the necessary rules
        RelOptPlanner planner = cluster.getPlanner();
        planner.addRule(CoreRules.PROJECT_TO_CALC);
        planner.addRule(CoreRules.FILTER_TO_CALC);
        planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SPOOL_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_UNION_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_MINUS_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_INTERSECT_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_MATCH_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_WINDOW_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE); // this allows to transform the LogicalTableScan into EnumerableTableScan


        // Define the type of the output plan (in this case we want a physical plan in
        // EnumerableContention)
        logPlan = planner.changeTraits(logPlan,
                cluster.traitSet().replace(EnumerableConvention.INSTANCE));
        planner.setRoot(logPlan);
        // Start the optimization process to obtain the most efficient physical plan based on the
        // provided rule set.
        EnumerableRel phyPlan = (EnumerableRel) planner.findBestExp();

        // Display the physical plan
        System.out.println(
                RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.NON_COST_ATTRIBUTES));

        // Obtain the executable plan
        Bindable<Object> executablePlan = EnumerableInterpretable.toBindable(
                new HashMap<>(),
                null,
                phyPlan,
                EnumerableRel.Prefer.ARRAY);

        // Run the executable plan using a context simply providing access to the schema
        System.out.println("[Query Output]");
        for (Object row : executablePlan.bind(new SchemaOnlyDataContext(schema, (JavaTypeFactory)typeFactory)))
        {
            if (row instanceof Object[]) {
                System.out.println(Arrays.toString((Object[]) row));
            } else {
                System.out.println(row);
            }
        }


//        TODO 2 - pouzit RelBuilder pro sestaveni logickeho planu
    }


    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }


    private static final class SchemaOnlyDataContext implements DataContext {
        private final SchemaPlus schema;
        private final JavaTypeFactory typeFactory;
        private final AtomicBoolean cancelFlag;

        SchemaOnlyDataContext(CalciteSchema calciteSchema, JavaTypeFactory typeFactory) {
            this.schema = calciteSchema.plus();
            this.typeFactory = typeFactory;
            this.cancelFlag = new AtomicBoolean(false); // Initialize cancelFlag here if needed
        }

        @Override
        public SchemaPlus getRootSchema() {
            return schema;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return typeFactory;
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public Object get(final String name) {
            switch (name) {
                case "cancelFlag":
                    return cancelFlag;
                // Handle other variables if needed
                default:
                    return null;
            }
        }
    }

}
