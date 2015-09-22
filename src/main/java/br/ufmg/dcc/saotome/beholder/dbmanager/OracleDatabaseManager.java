package br.ufmg.dcc.saotome.beholder.dbmanager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.CompositeDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.ext.oracle.Oracle10DataTypeFactory;
import org.dbunit.operation.DatabaseOperation;

public class OracleDatabaseManager {
	
	private static final String FULL_DB="FULLDB";
	
	/**
	 * Enumeração das extensões utilizadas para exportar informações sobre o banco de dados.
	 * @author icaroclever
	 *
	 */
	private enum ExtensionFile {
		XML(".xml")
		,ORD(".ord")
		,DTD(".dtd")
		;
		
		private String extension;
		private ExtensionFile(String extension) {
			this.extension = extension;
		}
		@Override
		public String toString() {
			return this.extension;
		}
	}

	/** Log da Classe */
	private final static Logger LOG = LogManager.getLogger(OracleDatabaseManager.class);

	/** Conexão JBDC. */
	private Connection jdbcConnection;

	/** Conexão do DBUnit. */
	private IDatabaseConnection connection;
	
	/** Caminho para salvar os arquivos exportados */
	private String path;
	
	/** Esquema do Banco de Dados */
	private String schema;
	
	/** Não trabalha com determinadas tabelas */
	private Set<String> skipedTables = new HashSet<String>();
	
	/** Mapa contendo a relação entre tabelas e restrições de integridade*/
	private final Map<String,List<String>> tableMap = new HashMap<String,List<String>>();

	/**
	 * Construtor que abre uma conexão com o banco de dados.
	 * 
	 * @param driver
	 *            Driver JDBC.
	 * @param url
	 *            URL JDBC para o banco de dados.
	 * @param username
	 *            Nome do Usuário do banco de dados.
	 * @param password
	 *            Senha do usuário do banco de dados.
	 * @param schema
	 *            Esquema do Banco de Dados a ser utilizado.
	 */
	public OracleDatabaseManager(final String driver, final String url, final String username, final String password, final String schema) {
		try {
			Class.forName(driver);
			jdbcConnection = DriverManager.getConnection(url, username,
					password);
			connection = new DatabaseConnection(jdbcConnection);
			connection.getConfig().setProperty("http://www.dbunit.org/features/qualifiedTableNames","true");
			connection.getConfig().setProperty("http://www.dbunit.org/features/skipOracleRecycleBinTables", "true");
			connection.getConfig().setProperty("http://www.dbunit.org/properties/datatypeFactory", new Oracle10DataTypeFactory());
		} catch (ClassNotFoundException e) {
			LOG.error("Driver JDBC não encontrado no classpath.");
			System.exit(1);
		} catch (SQLException e) {
			LOG.error(e.getMessage());
			System.exit(1);
		} catch (DatabaseUnitException e) {
			LOG.error("Não foi possível o DBUnit utilizar a conexão JDBC", e);
			System.exit(1);
		}
		if(schema == null || schema.isEmpty()){
			this.schema = username;
		}else {
			this.schema = schema;
		}
	}

	/**
	 * Construtor que abre uma conexão com o banco de dados.
	 * 
	 * @param driver
	 *            Driver JDBC.
	 * @param url
	 *            URL JDBC para o banco de dados.
	 * @param username
	 *            Nome do Usuário do banco de dados.
	 * @param password
	 *            Senha do usuário do banco de dados.
	 */
	public OracleDatabaseManager(final String driver, final String url,
			final String username, final String password) {
		this(driver, url, username, password, null);
	}

	/**
	 * Realiza a exportação da ordem de carregamento e dos dados das tabelas.
	 */
	public void fullExport() {
		exportOrder();
		exportData();
	}

	/** Extrai o nome da tabela do conjunto SCHEMA.TABLE_NAME */
	private String getTableName(final String string) {
		final Integer TABLE_NAME = 1;
		return string.split("\\.")[TABLE_NAME];
	}

	private String filePath(final String fileName, final ExtensionFile extension) {
		return path + File.separator + this.schema+ "."+ fileName + extension;
	}

	/**
	 * Cria o caminho de diretórios caso não exista no filesystem.
	 */
	private void createPathIfNotExists() {
		File dir = new File(this.path);
		if (!dir.exists()) {
			boolean created = dir.mkdirs();

			if (!created) {
				LOG.error("Não foi possível criar o diretório " + this.path);
				System.exit(1);
			}
		}
	}

	/**
	 * Exporta os dados das tabelas de em um conjunto de arquivos XML.
	 */
	public void exportData() {
		LOG.info("exportData: Iniciado em: " + Calendar.getInstance().getTime());
		createPathIfNotExists();
		try {
			for (String string : connection.createDataSet().getTableNames()) {
				String tableName = getTableName(string);
				if (string.contains(this.schema) && !this.skipedTables.contains(tableName)) {	
					QueryDataSet partialDataSet = new QueryDataSet(connection);
					partialDataSet.addTable(string);
					FlatXmlDataSet.write(partialDataSet, new FileOutputStream(filePath(tableName, ExtensionFile.XML)));
					LOG.info(String.format("Dados da Tabela %s exportada.", string));
				}
			}
		} catch (FileNotFoundException e) {
			LOG.error("Não é possível escrever no arquivo");
			System.exit(1);
		} catch (SQLException e) {
			LOG.error(e.getMessage());
			System.exit(1);
		} catch (IOException e) {
			LOG.error("Não é possível escrever no arquivo");
			System.exit(1);
		} catch (AmbiguousTableNameException e) {
			LOG.error(e.getMessage());
			System.exit(1);
		} catch (DataSetException e) {
			LOG.error(e.getMessage());
			System.exit(1);
		}
		LOG.info("exportData: Finalizado em: "
				+ Calendar.getInstance().getTime());
	}

	/**
	 * Exporta para cada tabela quais são suas tabelas dependentes. Cada tabela
	 * é representada por um arquivo .ord e cada linha desse arquivo consiste em
	 * uma tabela dependente.
	 */
	public void exportOrder() {
		LOG.info("exportOrder: Iniciado em: "
				+ Calendar.getInstance().getTime());
		createPathIfNotExists();
		
		try {
			for (String string : connection.createDataSet().getTableNames()) {
				String tableName = getTableName(string);
				if ((string.contains(this.schema) && !this.skipedTables.contains(tableName))) {
					PrintWriter writer = new PrintWriter(filePath(tableName,ExtensionFile.ORD), "UTF-8");
					writeDependencies(connection, writer, tableName);
					LOG.info(String.format("Ordem de carregamento da tabela %s exportada",string));
					writer.close();
				}
			}
		} catch (SQLException e) {
			LOG.error(e.getMessage());
			System.exit(1);
		} catch (FileNotFoundException e) {
			LOG.error("Não é possível escrever no arquivo");
			System.exit(1);
		} catch (UnsupportedEncodingException e) {
			LOG.error(e.getMessage());
			System.exit(1);
		} catch (DataSetException e) {
			LOG.error(e.getMessage());
			System.exit(1);
		}
		LOG.info("exportOrder: Finalizado em: "
				+ Calendar.getInstance().getTime());
	}

	/**
	 * Método recursivo que verifica as dependencias que as tabelas possuem entre si baseados em suas
	 * constraints.
	 * @param connection Conexão do DBUnit
	 * @param writer	Objeto onde está sendo escrita as dependências
	 * @param table		Tabela onde será buscada as dependências
	 * @throws DataSetException
	 * @throws SQLException
	 */
	private void writeDependencies(final IDatabaseConnection connection, final PrintWriter writer, final String table)
			throws DataSetException, SQLException {

		String query = String.format(
				  "select distinct TABLE_NAME "
				+ "from all_constraints " 
				+ "where r_constraint_name in ("
				+ "		select constraint_name " 
				+ "		from all_constraints "
				+ "		where table_name='%s' and owner = '%s')"
				, table
				, this.schema);

		ITable actualJoinData = connection.createQueryTable("RESULT_NAME",query);

		for (Integer index = 0; index < actualJoinData.getRowCount(); index++) {
			String dependencyTable = (String) actualJoinData.getValue(index,"TABLE_NAME");
			if (!table.equals(dependencyTable)) {
				writer.println(dependencyTable);
				writeDependencies(connection, writer, dependencyTable);
			}
		}
	}

	/**
	 * Limpa o banco de dados e carrega os dados contidos no XML da tabela.
	 * @param tableName Nome da tabela do banco de Dados a ser carregado 
	 */
	public void resetTableAndDependecies(String tableName) {
		
		if(this.skipedTables.contains(tableName)){
			LOG.warn(String.format("A tabela %s está na lista de tabelas ignoradas e não será processada.",tableName));
			return;
		}
		
		LOG.info("resetDB: Iniciado em: "+ Calendar.getInstance().getTime());
		try {
			ArrayList<IDataSet> dataSets = new ArrayList<IDataSet>();
			try {
				FlatXmlDataSetBuilder builder = new FlatXmlDataSetBuilder();
				builder.setColumnSensing(true);
				dataSets.add(builder.build(new File(filePath(tableName, ExtensionFile.XML))));
				LOG.info("Carregando o arquivo "+filePath(tableName, ExtensionFile.XML));

				Scanner scan;
				try {
					scan = new Scanner(new File(filePath(tableName, ExtensionFile.ORD)));
					while (scan.hasNext()) {
						String depTableName = scan.nextLine();
						dataSets.add(builder.build(new File(filePath(depTableName, ExtensionFile.XML))));
						LOG.info("Carregando o arquivo "+filePath(depTableName, ExtensionFile.XML));
					}
					CompositeDataSet compDataSet = new CompositeDataSet(dataSets.toArray(new FlatXmlDataSet[dataSets.size()]));
					DatabaseOperation.CLEAN_INSERT.execute(connection,compDataSet);
				} catch (FileNotFoundException e) {
					LOG.error(String.format("O arquivo %s não foi encontrado.",filePath(tableName, ExtensionFile.ORD)));
				}
			} catch (DatabaseUnitException e) {
				LOG.error("Tabela: " + tableName, e);
			} catch (MalformedURLException e) {
				LOG.error(String.format("O arquivo %s não foi encontrado.",	filePath(tableName, ExtensionFile.XML)));
			}
		} catch (SQLException e) {
			LOG.error("Tabela: " + tableName, e);
		}
		LOG.info("resetDB: Finalizado em: "
				+ Calendar.getInstance().getTime());
	}
	
	public void disableConstraints(){
		for (String tableName : tableMap.keySet()) {
			List<String> constraints = this.tableMap.get(tableName);
			for (String constraint : constraints) {
				disableContraint(tableName, constraint);
			}		
		}
	}
	
	public void enableConstraints(){
		for (String tableName : tableMap.keySet()) {
			List<String> constraints = this.tableMap.get(tableName);
			for (String constraint : constraints) {
				enableContraint(tableName, constraint);
			}		
		}
	}
	
	public void resetDB(){
		Scanner scan;
		try {
			scan = new Scanner(new File(filePath(FULL_DB, ExtensionFile.ORD)));
			while (scan.hasNext()) {
				String depTableName = scan.nextLine();
				if(!this.skipedTables.contains(depTableName)){
					resetTableAndDependecies(depTableName);	
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public void setSkipedTables(Set<String> skipedTables) {
		this.skipedTables = skipedTables;
	}
	
	public void addSkipedTable(String tableName){
		this.skipedTables.add(tableName);
	}
	
	public void removeSkipedTable(String tableName){
		this.skipedTables.remove(tableName);
	}

	@Override
	protected void finalize() throws Throwable {
		jdbcConnection.close();
	}

	/**
	 * @param path
	 *            Caminho onde os arquivos serão salvos. Se nulo ou em branco,
	 *            eles serão salvos no diretório corrente da execução da
	 *            aplicação.
	 */
	public void setPath(final String path) {
		if (path == null || path.isEmpty()) {
			this.path = System.getProperty("user.dir");
		} else {
			this.path = path;
		}
	}
	
	private void disableContraint(final String tableName, final String constraint ){
		final String query = String.format("alter table %s.%s DISABLE constraint %s", this.schema, tableName,constraint);
		
		Statement statement;
		try {
			statement = jdbcConnection.createStatement();
			statement.execute(query);
		} catch (SQLException e) {
			LOG.error("Problema ao executar a query: "+query);
		}
	}
	
	private void enableContraint(final String tableName, final String constraint ){
		final String query = String.format("alter table %s.%s ENABLE constraint %s", this.schema, tableName,constraint);
		
		Statement statement;
		try {
			statement = jdbcConnection.createStatement();
			statement.execute(query);
		} catch (SQLException e) {
			LOG.error("Problema ao executar a query: "+query);
		}
	}
	
	public void addConstraintToBeDisabled(String tableName, String constraint){
		if(!tableMap.containsKey(tableName)){
			this.tableMap.put(tableName, new ArrayList<String>());			
		}
		this.tableMap.get(tableName).add(constraint);
	}
	
	public void removeConstraintToBeDisabled(String tableName,String constraint){
		this.tableMap.get(tableName).remove(constraint);
	}
}

