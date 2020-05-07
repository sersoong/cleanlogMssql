package cleanlogMssql

import (
	"fmt"

	"log"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
)

//Server 服务器
type Server struct {
	Host     string
	Port     string
	Username string
	Password string
	DBName   string
	DB       *gorm.DB
}

//Log log表数据
type Log struct {
	Name string
}

//Connect 连接数据库
func (s *Server) Connect() error {
	var err error
	url := fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s&encrypt=disable", s.Username, s.Password, s.Host, s.Port, s.DBName)
	s.DB, err = gorm.Open("mssql", url)
	if err != nil {
		return err
	}
	return nil
}

//Close 关闭数据库
func (s *Server) Close() error {
	if s.DB != nil {
		err := s.DB.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

//Clean 清除日志
func (s *Server) Clean() error {
	if s.DB != nil {
		var l Log
		err := s.DB.Raw("SELECT name FROM sys.database_files WHERE TYPE_DESC='LOG';").Scan(&l).Error
		if err != nil {
			return err
		}
		fmt.Println("cleanning ", l.Name)
		err = s.DB.Exec("ALTER DATABASE " + s.DBName + " SET RECOVERY SIMPLE WITH NO_WAIT;").Error
		if err != nil {
			return err
		}
		err = s.DB.Exec("ALTER DATABASE " + s.DBName + " SET RECOVERY SIMPLE;").Error
		if err != nil {
			return err
		}
		err = s.DB.Exec("DBCC SHRINKFILE (N'" + l.Name + "', 11, TRUNCATEONLY);").Error
		if err != nil {
			return err
		}
		err = s.DB.Exec("ALTER DATABASE " + s.DBName + " set RECOVERY FULL with NO_WAIT;").Error
		if err != nil {
			return err
		}
		err = s.DB.Exec("ALTER DATABASE " + s.DBName + " set RECOVERY FULL;").Error
		if err != nil {
			return err
		}
	}
	return nil
}

//BatchClean 批量清除日志
func BatchClean(host, port, username, password string, dbs ...string) {
	var server Server
	server.Host = host
	server.Port = port
	server.Username = username
	server.Password = password
	for _, db := range dbs {
		server.DBName = db
		err := server.Connect()
		if err != nil {
			log.Fatalln(err.Error())
		}
		err = server.Clean()
		if err != nil {
			server.Close()
			log.Fatalln(err.Error())
		}
		server.Close()
	}
}
