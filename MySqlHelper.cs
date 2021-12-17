
using System;
using System.Collections;
using MySql.Data.MySqlClient;

namespace Database
{
    public class MySqlHelper
    {
        private static string connectionString = "";//连接字符串

        private MySqlConnection dbConnection;
        private MySqlCommand dbCommand;
        private MySqlDataReader reader;
        private MySqlTransaction transaction;


        public MySqlHelper(string connectionStr)
        {
            connectionString = connectionStr;
            Open(connectionString);
        }

        public MySqlConnection GetConnection()
        {
            MySqlConnection dbConnection;
            dbConnection = new MySqlConnection(connectionString);
            dbConnection.Open();
            return dbConnection;
        }

        public void CloseConnection()
        {
            return;
        }

        public MySqlHelper()
        {

        }

        public void Open(string connectionString)
        {
            try
            {
                dbConnection = new MySqlConnection(connectionString);
                dbConnection.Open();
            }
            catch (Exception e)
            {
                //string temp1 = e.ToString();
            }
        }

        public void Close()
        {
            if (dbCommand != null)
            {
                dbCommand.Dispose();
            }

            dbCommand = null;

            if (reader != null)
            {
                reader.Dispose();
            }

            reader = null;

            if (dbConnection != null)
            {
                dbConnection.Close();
            }

            dbConnection = null;
        }

        public MySqlDataReader ExecuteQuery(string sqlQuery)
        {
            ReOpen();

            dbCommand = dbConnection.CreateCommand();
            dbCommand.CommandText = sqlQuery;
            reader = dbCommand.ExecuteReader();

            CloseConnection();
            return reader;
        }

        public MySqlDataReader ReadFullTable(string tableName)
        {
            string query = "SELECT * FROM " + tableName;

            return ExecuteQuery(query);
        }


        public MySqlDataReader ReadRecord(string tableName, string key, int id)
        {
            string query = "SELECT * FROM " + tableName + " where " + key + " = " + id.ToString();

            return ExecuteQuery(query);
        }

        //public MySqlDataReader ReadRecord(string tableName, int id)
        //{
        //    //string query = "SELECT * FROM " + tableName + " where " + key + " = " + id.ToString();

        //    //return ExecuteQuery(query);
        //}

        public MySqlDataReader Insert(string tableName, string[] values)
        {
            string query = "INSERT INTO " + tableName + " VALUES (" + values[0];

            for (int i = 1; i < values.Length; ++i)
            {
                query += ", " + values[i];
            }
            query += ")";

            return ExecuteQuery(query);
        }

        public MySqlDataReader Update(string tableName, string[] cols, string[] colsvalues, string selectkey, string selectvalue)
        {
            string query = "UPDATE " + tableName + " SET " + cols[0] + " = " + colsvalues[0];
            for (int i = 1; i < colsvalues.Length; ++i)
            {
                query += ", " + cols[i] + " =" + colsvalues[i];
            }
            query += " WHERE " + selectkey + " = " + selectvalue + " ";

            return ExecuteQuery(query);
        }

        public MySqlDataReader Update(string tableName, string[] cols, string[] colsvalues, string[] selectkey, string[] selectvalue)
        {
            string query = "UPDATE " + tableName + " SET " + cols[0] + " = " + colsvalues[0];
            for (int i = 1; i < colsvalues.Length; ++i)
            {
                query += ", " + cols[i] + " =" + colsvalues[i];
            }

            query += " WHERE " + selectkey[0] + " = " + selectvalue[0] + " ";
            for (int i = 1; i < selectkey.Length; ++i)
            {
                query += "and " + selectkey[i] + " =" + selectvalue[i];
            }

            return ExecuteQuery(query);
        }


        public MySqlDataReader Delete(string tableName, string[] cols, string[] colsvalues)
        {
            string query = "DELETE FROM " + tableName + " WHERE " + cols[0] + " = " + colsvalues[0];
            for (int i = 1; i < colsvalues.Length; ++i)
            {
                query += " or " + cols[i] + " = " + colsvalues[i];
            }

            return ExecuteQuery(query);
        }

        public MySqlDataReader InsertSpecific(string tableName, string[] cols, string[] values)
        {
            if (cols.Length != values.Length)
            {
                throw new Exception("columns.Length != values.Length");
            }

            string query = "INSERT INTO " + tableName + "(" + cols[0];
            for (int i = 1; i < cols.Length; ++i)
            {
                query += ", " + cols[i];
            }
            query += ") VALUES (" + values[0];

            for (int i = 1; i < values.Length; ++i)
            {
                query += ", " + values[i];
            }
            query += ")";

            return ExecuteQuery(query);
        }

        public MySqlDataReader Delete(string tableName)
        {
            string query = "DELETE FROM " + tableName;

            return ExecuteQuery(query);
        }

        public MySqlDataReader CreateTable(string name, string[] col, string[] colType)
        {
            GetConnection();

            if (col.Length != colType.Length)
            {
                throw new Exception("columns.Length != colType.Length");
            }

            string query = "CREATE TABLE " + name + " (" + col[0] + " " + colType[0];
            for (int i = 1; i < col.Length; ++i)
            {
                query += ", " + col[i] + " " + colType[i];
            }
            query += ")";

            return ExecuteQuery(query);
        }

        public MySqlDataReader SelectWhere(string tableName, string[] items, string[] col, string[] operation, string[] values)
        {
            GetConnection();
            if (col.Length != operation.Length || operation.Length != values.Length)
            {
                throw new Exception("col.Length != operation.Length != values.Length");
            }

            string query = "SELECT " + items[0];
            for (int i = 1; i < items.Length; ++i)
            {
                query += ", " + items[i];
            }

            query += " FROM " + tableName + " WHERE " + col[0] + operation[0] + "'" + values[0] + "' ";
            for (int i = 1; i < col.Length; ++i)
            {
                query += " AND " + col[i] + operation[i] + "'" + values[0] + "' ";
            }

            return ExecuteQuery(query);
        }


        public void ReOpen()
        {
            Close();
            Open(connectionString);
        }

        public MySqlTransaction BeginTransaction(string query)
        {
            dbCommand = dbConnection.CreateCommand();
            dbCommand.CommandText = query;
            transaction = dbConnection.BeginTransaction();
            dbCommand.ExecuteNonQuery();

            return transaction;
        }

        public void Commit()
        {
            transaction.Commit();
        }

        public void Rollback()
        {
            transaction.Rollback();
        }

        public bool Transaction(string query)
        {
            try
            {
                BeginTransaction(query);
                Commit();
            }
            catch
            {
                Rollback();
                return false;
            }
            return true;
        }

    }
}