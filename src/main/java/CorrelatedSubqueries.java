import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class CorrelatedSubqueries {

	public static void main(String[] args) throws ValidationException, RelConversionException, SqlParseException, SQLException {
		convertSql("SELECT id, friend_id,\n" +
		           " (SELECT max(f2.id)\n" +
		           "  FROM friends AS f2\n" +
		           "  WHERE f2.friend_id = f1.friend_id) AS foaf_id\n" +
		           "FROM friends AS f1");
		convertSql("SELECT id, friend_id,\n" +
		           " (SELECT id\n" +
		           "  FROM friends as f2\n" +
		           "  WHERE f2.friend_id = f1.friend_id) AS friend\n" +
		           "FROM friends as f1\n" +
		           "WHERE id = '203'");
	}

	private static void convertSql(final String theSql) throws SQLException, SqlParseException, ValidationException, RelConversionException {
		final DataSource aDataSource = JdbcSchema.dataSource("jdbc:mysql://localhost:3306/stardog?allowMultiQueries=true&useSSL=false", "com.mysql.jdbc.Driver", "root", "admin");
		try (Connection aConnection = connect(aDataSource)) {
			SqlDialectFactory aSqlDialectFactory = new SqlDialectFactoryImpl();
			final SqlDialect aSqlDialect = aSqlDialectFactory.create(aConnection.getMetaData());
			final SqlDialect.DatabaseProduct aDatabaseProduct = SqlDialect.getProduct(aConnection.getMetaData().getDatabaseProductName(), "UNUSED");
			String schemaName = getSchemaName(aDatabaseProduct, aConnection);
			String catalog = aConnection.getCatalog();
			FrameworkConfig aConfig = createConfig(aDataSource, aDatabaseProduct, catalog, schemaName);
			Planner aPlanner = Frameworks.getPlanner(aConfig);
			SqlNode aQuery = aPlanner.parse(theSql);
			aQuery = aPlanner.validate(aQuery);
			RelNode aRelNode = aPlanner.rel(aQuery).project();
			RelToSqlConverter aSqlConverter = new RelToSqlConverter(aSqlDialect);
			SqlNode aSqlNode = aSqlConverter.visitChild(0, aRelNode).asStatement();
			System.out.println(theSql);
			System.out.println(RelOptUtil.toString(aRelNode));
			System.out.println(aSqlNode);
		}
	}

	private static FrameworkConfig createConfig(final DataSource theDataSource, final SqlDialect.DatabaseProduct theDatabaseProduct, final String theCatalog, final String theSchemaName) {
		SchemaPlus aRootSchema = Frameworks.createRootSchema(true);
		aRootSchema.add(theSchemaName, JdbcSchema.create(aRootSchema, theSchemaName, theDataSource, theCatalog, theSchemaName));
		return Frameworks.newConfigBuilder()
		                 .parserConfig(buildParserConfig(theDatabaseProduct, "ANSI"))
		                 .defaultSchema(aRootSchema.getSubSchema(theSchemaName))
		                 .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
		                 .context(Contexts.EMPTY_CONTEXT)
		                 .typeSystem(RelDataTypeSystem.DEFAULT)
		                 .build();
	}

	private static String getSchemaName(final SqlDialect.DatabaseProduct theDatabaseProduct, final Connection conn) throws SQLException {
		if (theDatabaseProduct == SqlDialect.DatabaseProduct.MYSQL) {
			return "stardog";
		}
		else {
			// this works at least on Oracle, MSSQL, Postgres
			String schemaName = conn.getSchema();
			// H2 doesn't return a value from conn.getSchema()
			if (schemaName == null) {
				schemaName = "PUBLIC";
			}
			return schemaName;
		}
	}

	private static Connection connect(final DataSource theDataSource) {
		try {
			return theDataSource.getConnection();
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static SqlParser.Config buildParserConfig(SqlDialect.DatabaseProduct dbms, String parserSqlQuoting) {
		SqlParser.ConfigBuilder aParserConfig = SqlParser.configBuilder();
		// c.f. https://github.com/ontop/ontop/wiki/Case-sensitivity-for-SQL-identifiers
		switch (dbms) {
			// TODO : same rules for IMPALA ; http://www.cloudera.com/documentation/archive/impala/2-x/2-1-x/topics/impala_identifiers.html
			case HIVE:
			case MYSQL:
				aParserConfig.setLex(Lex.MYSQL);
				break;
			case POSTGRESQL:
				// c.f. [CALCITE-1614] for Lex.POSTGRESQL
				aParserConfig.setCaseSensitive(false)
				             .setQuoting(Quoting.DOUBLE_QUOTE)
				             .setQuotedCasing(Casing.UNCHANGED)
				             .setUnquotedCasing(Casing.TO_LOWER);
				break;
			case MSSQL:
				aParserConfig.setLex(Lex.SQL_SERVER);
				break;
			default:
				aParserConfig.setLex(Lex.ORACLE);
				break;
		}
		if ("ANSI".equals(parserSqlQuoting)) {
			aParserConfig.setQuoting(Quoting.DOUBLE_QUOTE);
		}
		return aParserConfig.build();
	}
}