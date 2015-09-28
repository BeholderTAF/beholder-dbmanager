package br.ufmg.dcc.saotome.beholder.dbmanager;

/**
 * <p>Classe executável para executar via terminal o DBManager. Para isso, deve-se passar os seguintes parâmetros
 * para a aplicação:</p>
 * <ul>
 * 		<li><b>db.driver:</b> JDBC Driver do Banco de Dados.</li>
 * 		<li><b>db.url:</b> JDBC URL para conexão com o Banco de Dados.</li>
 * 		<li><b>db.user:</b> Usuário do Banco de Dados</li>
 * 		<li><b>db.password:</b> Senha do Banco de Dados</li>
 * 		<li><b>db.schema:</b> Esquema do Banco de Dados</li>
 * 		<li><b>action:</b> Ação a ser executada (EXPORT_DATA,EXPORT_ORDER,FULL_EXPORT,RELOAD_DB ou RELOAD_TABLE)</li>
 * 		<li><b>table: Nome da Tabela a ser recarregada pela ação RELOAD_TABLE</b></li>
 * 		<li>(opcional)<b>export.dir:</b> diretório onde armazenará ou está armazenado os arquivos .xml e .ord</li>
 * 		<li>(opcional)<b>disable.constraints:</b> String contendo as constraints a serem desabilitadas do banco antes de ser feito um reload. 
 * 				O padrão é NOME_DA_TABELA1:CONSTRAINT1,NOME_DA_TABELA2:CONSTRAINT2 ...</li>
 * </ul>
 * @author icaroclever
 *
 */
public class Main {
	
	private static final String DBDRIVER="db.driver"
								,DBURL="db.url"
								,DBUSER="db.user"
								,DBPASSWORD="db.password"
								,DBSCHEMA="db.schema"
								,ACTION="action"
								,TABLE_NAME="table"
								,EXPORT_DIR="export.dir"
								,DISABLE_CONSTRAINTS="disable.constraints"
								;
	
	private enum Actions {
		 EXPORT_DATA
		,EXPORT_ORDER
		,FULL_EXPORT
		,RELOAD_DB
		,RELOAD_TABLE
		;
		 
		public static Actions toActions(String string){
			return valueOf(string.toUpperCase());
		}
	}
	
	public static OracleDatabaseManager loadDatabaseManager(){
		String 	 driver 	= System.getProperty(DBDRIVER)
				,url 		= System.getProperty(DBURL)
				,username	= System.getProperty(DBUSER)
				,password	= System.getProperty(DBPASSWORD)
				,schema		= System.getProperty(DBSCHEMA)
				;
		return new OracleDatabaseManager(driver, url, username, password, schema);
				
	}
	
	private static void execute(OracleDatabaseManager dbManager){
		Actions action = Actions.toActions(System.getProperty(ACTION));
		processDisabledConstraints(dbManager);
		dbManager.setPath(System.getProperty(EXPORT_DIR));
		switch (action){
			case EXPORT_DATA:
				dbManager.exportData();
				break;
			case EXPORT_ORDER:
				dbManager.exportOrder();
				break;
			case FULL_EXPORT:
				dbManager.fullExport();
				break;
			case RELOAD_DB:
				dbManager.resetDB();
				break;
			case RELOAD_TABLE:
				dbManager.disableConstraints();
				dbManager.resetTableAndDependecies(System.getProperty(TABLE_NAME));
				dbManager.enableConstraints();
				break;
		}
	}
	
	private static void processDisabledConstraints(OracleDatabaseManager dbManager){
		String disabledConstraintsString = System.getProperty(DISABLE_CONSTRAINTS);
		
		final int TABLE_NAME_INDEX = 0;
		final int CONSTRAINT_NAME_INDEX=1;
		
		String[] disabledConstraints = disabledConstraintsString.split(",");
		
		for (String disabledConstraintString : disabledConstraints) {
			String[] disabledConstraint = disabledConstraintString.split(":");
			dbManager.addConstraintToBeDisabled(disabledConstraint[TABLE_NAME_INDEX], disabledConstraint[CONSTRAINT_NAME_INDEX]);
		}
	}
	
	public static void main(String[] args){
		OracleDatabaseManager dbManager = loadDatabaseManager();
		execute(dbManager);
	}

}
