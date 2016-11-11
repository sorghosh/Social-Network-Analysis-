from sqlite3 import dbapi2 as sqlite
import random
import datetime


class social_media:
    def __init__(self,dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()
    def db_commit(self):
        self.con.commit()
    def create_table(self):
        self.con.execute("create table user (username,joining_date)")
        self.con.execute("create table user_connection(user_con_id integer , username text ,default_id integer )")
        self.con.execute("create table friends(primary_id  , secondary_id  , friendship_date)")
        self.db_commit()
    
    def get_date(self):
        current_time = datetime.datetime.now().strftime("%m/%d/%y")
        add_date  = random.randint(0,5)
        get_date  = datetime.datetime.strptime(current_time,"%m/%d/%y") + datetime.timedelta(days = add_date)
        get_date  = get_date.strftime("%m/%d/%y")
        return get_date

    def db_newusers(self,username):
        get_date  = self.get_date()                
        cur = self.con.execute("select * from user where username = (?)",(username,))
        rec = cur.fetchone()
        if rec == None or  rec[0] == 0:
            self.con.execute("insert into user values (?,?)",(username,get_date))
            user_con_id_cur = self.con.execute("select rowid,username from user").fetchall()
            if len(user_con_id_cur) == 1:
                self.con.execute("insert into user_connection values (?,?,?)",(user_con_id_cur[0][0],username,1))
            else:
                user_con_id_cur = user_con_id_cur[len(user_con_id_cur)-1][0]
                self.con.execute("insert into user_connection values (?,?,?)",(user_con_id_cur,username,1))           
            self.db_commit()
            
    def add_freinds(self,f1,f2):
        get_date = self.get_date()
        cur_f1 = self.con.execute("select count(*) from user where username in (?)",(f1,))
        cur_f2 = self.con.execute("select count(*) from user where username in (?)",(f2,))
        rec_f1 = cur_f1.fetchone()
        rec_f2 = cur_f2.fetchone()

        if rec_f1 == None or rec_f1[0] == 0 or rec_f2 == None or rec_f2[0] == 0 :
            print "users don't exists in the database"
        else:
            f1_rowid = self.con.execute("select rowid from user where username = (?)",(f1,)).fetchone()
            f2_rowid = self.con.execute("select rowid from user where username= (?)",(f2,)).fetchone()
            f1_rowid = f1_rowid[0]
            f2_rowid = f2_rowid[0]
            self.con.execute("insert into friends values(?,?,?)",(f1_rowid,f2_rowid,get_date))
            self.union(f1,f2)
            self.db_commit()

    def union(self,f1,f2):
        i = self.root(f1)
        y = self.root(f2)
        print i
        print y
        node1_size = self.con.execute("select default_id from user_connection where rowid = (?)",(i,)).fetchone()
        node1_size = node1_size[0]
        node2_size = self.con.execute("select default_id from user_connection where rowid = (?)",(y,)).fetchone()
        node2_size = node2_size[0]
        
        if node1_size < node2_size:
            self.con.execute("update user_connection set user_con_id = (?) where rowid = (?)",(y,i))
            self.con.execute("update user_connection set default_id = (?) where rowid = (?)",(node1_size + node2_size,i))
            self.db_commit()
        else:
            self.con.execute("update user_connection set user_con_id = (?) where rowid = (?)",(y,i))
            self.con.execute("update user_connection set default_id = (?) where rowid = (?)",(node1_size + node2_size,i))
            self.db_commit()
            
    def root(self,r):
        cur_connectionid = self.con.execute("select rowid, user_con_id from user_connection where username = (?)",(r,)).fetchone()
        rowid        = cur_connectionid[0]
        user_con_id  = cur_connectionid[1]
#        print rowid , user_con_id
        while (user_con_id != rowid):
            rowid       = self.con.execute("select user_con_id from user_connection where user_con_id = (?)",(user_con_id,)).fetchone()
            rowid       = rowid[0]
            user_con_id = self.con.execute("select user_con_id from user_connection where rowid = (?)",(rowid,)).fetchone() 
            user_con_id = user_con_id[0]
        return user_con_id
        
        
    def connected(self,f1,f2):
        i = self.root(f1)
        y = self.root(f2)
#        print i
#        print y
        return i == y
        
    
    def print_connected(self):
        pass

dbname = "C:\Users\sauravghosh\Desktop\MachineLearning\DataStructure\Quick_Union\Code\NewtWorkAnalysis\social_media.db"
obj = social_media(dbname)

#####adding user to the database
#obj.db_newusers("adam")
#obj.db_newusers("allen")
#obj.db_newusers("ashwini")
#obj.db_newusers("ghosh")
#obj.db_newusers("john")
#obj.db_newusers("parker")
#obj.db_newusers("saurav")
#obj.db_newusers("sean")
#obj.db_newusers("sachin")
#obj.db_newusers("rahul")


#
obj.add_freinds("john","ghosh")
obj.add_freinds("john","sachin")
obj.add_freinds("saurav","parker")
obj.add_freinds("rahul","allen")
obj.add_freinds("ashwini","adam")
obj.add_freinds("parker","adam")
obj.add_freinds("sean","ashwini")
obj.add_freinds("saurav","allen")
obj.add_freinds("sean","ghosh")
