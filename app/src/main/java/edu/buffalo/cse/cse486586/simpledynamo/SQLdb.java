package edu.buffalo.cse.cse486586.simpledynamo;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteConstraintException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity.REMOTE_PORT0;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity.TAG;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity.delim;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity.getnode;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity.hash_to_port;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity.myPort;
import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity.topology;

/**
 * Created by Ankesh N. Bhoi on 02/11/2018.
 */

public class SQLdb extends SQLiteOpenHelper {

    int message=1,port=2,type=0;

    public static final String db_name = "kvalpair.db";
    public static final String tbl_name = "kvalpair";


    public SQLdb(Context context) {
        super(context, db_name, null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        sqLiteDatabase.execSQL("create table " + tbl_name + " (`key` TEXT PRIMARY KEY,`value` TEXT,`version` INTEGER ,`replica` INTEGER)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {
        sqLiteDatabase.execSQL("DROP TABLE IF EXISTS kvalpairs.db");
        onCreate(sqLiteDatabase);
    }

    public void insert(ContentValues values,int force) {

            if (force == 1) {//remote insert
                SQLiteDatabase db = this.getWritableDatabase();
                Object key = values.get("key");
                Object val = values.get("value");
                try {
                    Cursor query = db.rawQuery("select `key` from " + tbl_name + " WHERE `key` = '" + key + "'", null);
                    if (query.moveToFirst()) {
                        do {
                            if (key.equals(query.getString(query.getColumnIndex("key")))) {
                                db.execSQL("UPDATE " + tbl_name + " SET version = version + 1, value = '" + val + "'  WHERE key = '" + String.valueOf(key) + "'");
                            }

                        } while (query.moveToNext());
                    } else {
                        values.put("replica", Integer.valueOf(myPort));
                        values.put("version", 0);
                        //Log.v("insert from other avd", val.toString());
                        db.insert(tbl_name, null, values);
                    }
                    query.close();
                } catch (SQLiteConstraintException e) {
                    e.printStackTrace();
                }
                //db.delete(tbl_name, "key=" + "'" + key + "'", null);
                //final long check = db.insert(tbl_name, null, values);
                //Log.v("insert", values.toString() + " force");
            } else if ((SimpleDynamoActivity.nxt_node).equals(SimpleDynamoActivity.my_avd_hash)) {//only one avd
                SQLiteDatabase db = this.getWritableDatabase();
                //db.delete(tbl_name, "key=" + "'" + key + "'", null);
                //final long check = db.insert(tbl_name, null, values);
                Object key = values.get("key");
                Object val = values.get("value");

                try {
                    Cursor query = db.rawQuery("select `key` from " + tbl_name + " WHERE `key` = '" + key + "'", null);
                    if (query.moveToFirst()) {
                        do {
                            if (key.equals(query.getString(query.getColumnIndex("key")))) {
                                db.execSQL("UPDATE " + tbl_name + " SET version = version + 1, value = '" + val + "'  WHERE key = '" + String.valueOf(key) + "'");
                            }

                        } while (query.moveToNext());
                    } else {
                        values.put("replica", Integer.valueOf(myPort));
                        values.put("version", 0);
                        //Log.v("insert from other avd", val.toString());
                        db.insert(tbl_name, null, values);
                    }
                    query.close();
                } catch (SQLiteConstraintException e) {
                    e.printStackTrace();
                }

                //Log.v("insert", values.toString() + " one node");
            } else if (SimpleDynamoProvider.genHash(values.get("key").toString()).equals(SimpleDynamoActivity.my_avd_hash)) {//key hash=myhash
                SQLiteDatabase db = this.getWritableDatabase();
                Object key = values.get("key");
                Object val = values.get("value");
                try {
                    Cursor query = db.rawQuery("select `key` from " + tbl_name + " WHERE `key` = '" + key + "'", null);
                    if (query.moveToFirst()) {
                        do {
                            if (key.equals(query.getString(query.getColumnIndex("key")))) {
                                db.execSQL("UPDATE " + tbl_name + " SET version = version + 1, value = '" + val + "'  WHERE key = '" + String.valueOf(key) + "'");
                            }

                        } while (query.moveToNext());
                    } else {
                        values.put("replica", Integer.valueOf(myPort));
                        values.put("version", 0);
                        //Log.v("insert from other avd", val.toString());
                        db.insert(tbl_name, null, values);
                    }
                    query.close();
                } catch (SQLiteConstraintException e) {
                    e.printStackTrace();
                }
                //Log.v("insert", values.toString() + "key hash same");
            } else if (force != 0 && force != 1) {//replica
                SQLiteDatabase db = this.getWritableDatabase();
                Object key = values.get("key");
                Object val = values.get("value");
                try {
                    Cursor query = db.rawQuery("select `key` from " + tbl_name + " WHERE `key` = '" + key + "'", null);
                    if (query.moveToFirst()) {
                        do {
                            if (key.equals(query.getString(query.getColumnIndex("key")))) {
                                db.execSQL("UPDATE " + tbl_name + " SET version = version + 1, value = '" + val + "'  WHERE key = '" + String.valueOf(key) + "'");
                            }

                        } while (query.moveToNext());
                    } else {
                        values.put("replica", force);
                        values.put("version", 0);
                        //Log.v("insert from other avd", val.toString());
                        db.insert(tbl_name, null, values);
                    }
                    query.close();
                } catch (SQLiteConstraintException e) {
                    e.printStackTrace();
                }
                //Log.v("insert", values.toString() + "key hash same");
            } else {//insert at other avd
                List<String> position_finder = new ArrayList<String>(topology);
                position_finder.add(SimpleDynamoProvider.genHash(values.get("key").toString()));
                Collections.sort(position_finder);
                if (position_finder.indexOf(SimpleDynamoProvider.genHash(values.get("key").toString())) == position_finder.size() - 1) {//if last index
                    //send key val to port
                    //Log.v("remote insert", values.toString() + hash_to_port.get(position_finder.get(0)));
                    new send().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", values.get("key").toString() + delim + values.get("value").toString(), hash_to_port.get(position_finder.get(0)));

                } else {
                    int pos = position_finder.indexOf(SimpleDynamoProvider.genHash(values.get("key").toString()));
                    //send key val to port
                    //Log.v("remote insert", values.toString() + hash_to_port.get(position_finder.get(pos + 1)));
                    new send().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", values.get("key").toString() + delim + values.get("value").toString(), hash_to_port.get(position_finder.get(pos + 1)));
                }
                position_finder.remove(SimpleDynamoProvider.genHash(values.get("key").toString()));
            }

        }



    public Cursor query_response(String selection,int force) throws ExecutionException, InterruptedException {

        if (force==1) {//query from server
            SQLiteDatabase db = this.getWritableDatabase();
            Cursor res = db.rawQuery("select key,value from " + tbl_name+ " WHERE `key`= " + "'" + selection + "'", null);
            //res.moveToFirst();
            int x = res.getCount();
            return res;
        }
        else if ((selection.equals("*") || selection.equals("@")) && (SimpleDynamoActivity.nxt_node).equals(SimpleDynamoActivity.my_avd_hash)) {//one node
            SQLiteDatabase db = this.getWritableDatabase();
            Cursor res = db.rawQuery("select `key`,value from " + tbl_name, null);
            res.moveToFirst();
            int x = res.getCount();
            return res;
        }
        else if (selection.equals("@")) {//multiple nodes
            SQLiteDatabase db = this.getWritableDatabase();
            Cursor res = db.rawQuery("select `key`,value from " + tbl_name, null);

            //Cursor res = db.rawQuery("select `key`,value from " + tbl_name+" Where replica = "+String.valueOf(getnode(myPort,-1))+" or replica = "+String.valueOf(getnode(myPort,-2))+" or replica = "+String.valueOf(getnode(myPort,-1)), null);
            //res.moveToFirst();
            int x = res.getCount();
            return res;
        }
        else if ((SimpleDynamoActivity.nxt_node).equals(SimpleDynamoActivity.my_avd_hash)) {//my hash == key hash
            SQLiteDatabase db = this.getWritableDatabase();
            Cursor res = db.rawQuery("select `key`,value from " + tbl_name + " WHERE `key`= " + "'" + selection + "'", null);
            res.moveToFirst();
            return res;
        }
         else if(selection.equals("*")){
            return  new send().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "*",selection,myPort).get();
        }
        else if(force!=0&&force!=1){
            SQLiteDatabase db = this.getWritableDatabase();
            Cursor res = db.rawQuery("select `key`,value from " + tbl_name , null);
            res.moveToFirst();
            return res;
        }
        else if(selection.equals("i dont know why i need this but return null at the end stops working if i dont do this")){}//hack
        else {
            List<String>position_finder=new ArrayList<String>(topology);

            position_finder.add(SimpleDynamoProvider.genHash(selection));
            Collections.sort(position_finder);


            if (position_finder.indexOf(SimpleDynamoProvider.genHash(selection)) == position_finder.size() - 1){//query node 0
                //if last index
                //Log.v("remote query",selection+ hash_to_port.get(position_finder.get(0)));
                String prt= hash_to_port.get(position_finder.get(0));
                position_finder.remove(SimpleDynamoProvider.genHash(selection));
                return  new send().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query",selection,prt).get();

            }
            else{//any other index query next node of selection hash
                int pos = position_finder.indexOf(SimpleDynamoProvider.genHash(selection.toString()));
                //send key val to port
                //Log.v("remote query",selection+ hash_to_port.get(position_finder.get(pos+1)));
                String prt= hash_to_port.get(position_finder.get(pos + 1));
                position_finder.remove(SimpleDynamoProvider.genHash(selection));
                return new send().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", selection, prt).get();
            }

        }
        return null;
    }

    public void delete(String selection) {
        String key = selection;
        SQLiteDatabase db = this.getWritableDatabase();
        //db.delete(tbl_name, "key=" + "'" + key + "'", null);
        db.delete(tbl_name,null,null);
        if(myPort.equals(REMOTE_PORT0))
        new send().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete");
    }

    private class send extends AsyncTask<String, Void, Cursor> {

        @Override
        protected Cursor doInBackground(String... msgs) {
                List<String> top=new ArrayList<String>(topology);
                String nullexmsg=null;
                try {
                    if (msgs[type].equals("*")) {
                        String[] rows = {"key", "value"};
                        MatrixCursor c = new MatrixCursor(rows);

                        //for(int i =Integer.parseInt(SimpleDynamoActivity.REMOTE_PORT0);i<=11124;i=i+4){
                        for (String i : top) {
                            try {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(hash_to_port.get(i)));
                                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                                bw.write("*" + "\n");
                                bw.flush();
                                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                String keys = br.readLine();
                                //Log.v("*k", keys);
                                String values = br.readLine();
                                //Log.v("*v", values);
                                String[] k = keys.split(delim);
                                String[] v = values.split(delim);
                                //Log.v("kval", "klen " + k.length + " vlen " + v.length);
                                for (int j = 0; j < k.length; j++) {
                                    c.addRow(new Object[]{k[j], v[j]});
                                }
                            }catch (IOException e){
                                //Log.v("* handling Dead Avd","querying next node");
                                e.printStackTrace();
                            }catch (NullPointerException e){
                                e.printStackTrace();
                            }

                        }
                        return c;
                    }else if(msgs[type].equals("delete")){
                        for (String i : top) {
                            try {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(hash_to_port.get(i)));
                                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                                bw.write("delete" + "\n");                                bw.flush();

                            }catch (IOException e){
                                e.printStackTrace();
                            }

                        }

                    }
                    else if (msgs.length == 3) {
                        //Log.v("sender up", msgs[message]);
                        Socket socket=new Socket();
                        int ji = Integer.parseInt(msgs[port]);
                        socket.connect(new InetSocketAddress("10.0.2.2",ji),100);
                        String msgToSend = msgs[type] + delim + msgs[message];
                        if (msgs[type].equals("insert")) {
                            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                            bw.write(msgToSend + "\n");
                            bw.flush();

                            socket=new Socket();
                            ji = getnode(msgs[port],1);
                            socket.connect(new InetSocketAddress("10.0.2.2",ji),50);
                            bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                            bw.write("insert_replica" +delim+msgs[message]+ "\n");
                            bw.flush();

                            socket=new Socket();
                            ji = getnode(msgs[port],2);
                            socket.connect(new InetSocketAddress("10.0.2.2",ji),50);
                            bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                            bw.write("insert_replica" +delim+msgs[message]+ "\n");
                            bw.flush();

                            //Log.v("send", msgToSend);
                        } else if (msgs[type].equals("query")) {
                            try {
                                nullexmsg = msgToSend;
                                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                                bw.write(msgToSend + "\n");
                                bw.flush();
                                //Log.v("send", msgToSend);

                                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                String received_message = br.readLine();
                                String[] str = received_message.split(delim);
                                Cursor c;
                                String[] stri = {"key", "value"};
                                MatrixCursor mc = new MatrixCursor(stri);
                                mc.addRow(new Object[]{str[0], str[1]});
                                return mc;
                            }catch (ArrayIndexOutOfBoundsException e){}
                        }
                    }
                }catch (IOException e) {
                    //Log.v("handling Dead Avd", "Send task socket IOException");
                    try{
                        if (msgs.length == 3) {
                            //Log.v("sender up", msgs[message]);
                            Socket socket=new Socket();
                            int ji = Integer.parseInt(msgs[port]);
                            String msgToSend = null;
                            if (msgs[type].equals("insert")) {
                                socket.connect(new InetSocketAddress("10.0.2.2",SimpleDynamoActivity.getnode(String.valueOf(ji),1)),100);
                                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                                msgToSend="insert_backup" + delim + msgs[message];
                                bw.write(msgToSend + "\n");
                                bw.flush();
                                //Log.v("send_backup", msgToSend);
                            } else if (msgs[type].equals("query")) {
                                socket.connect(new InetSocketAddress("10.0.2.2",SimpleDynamoActivity.getnode(String.valueOf(ji),1)),100);
                                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                                msgToSend=msgs[type] + delim + msgs[message];
                                bw.write(msgToSend + "\n");
                                bw.flush();
                                //Log.v("send", msgToSend);

                                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                String received_message = br.readLine();
                                String[] str = received_message.split(delim);
                                Cursor c;
                                String[] stri = {"key", "value"};
                                MatrixCursor mc = new MatrixCursor(stri);
                                mc.addRow(new Object[]{str[0], str[1]});
                                return mc;
                            }
                        }
                    }catch (IOException e1) {
                        try {
                            if (msgs.length == 3) {
                                //Log.v("sender up", msgs[message]);
                                Socket socket = new Socket();
                                int ji = Integer.parseInt(msgs[port]);
                                String msgToSend = null;
                                if (msgs[type].equals("insert")) {
                                    socket.connect(new InetSocketAddress("10.0.2.2", SimpleDynamoActivity.getnode(String.valueOf(ji), 2)), 100);
                                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                                    msgToSend = "insert_backup" + delim + msgs[message];
                                    bw.write(msgToSend + "\n");
                                    bw.flush();
                                    //Log.v("send_backup", msgToSend);
                                } else if (msgs[type].equals("query")) {
                                    socket.connect(new InetSocketAddress("10.0.2.2", SimpleDynamoActivity.getnode(String.valueOf(ji), 2)), 100);
                                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                                    msgToSend = msgs[type] + delim + msgs[message];
                                    bw.write(msgToSend + "\n");
                                    bw.flush();
                                    //Log.v("send", msgToSend);

                                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                    String received_message = br.readLine();
                                    String[] str = received_message.split(delim);
                                    Cursor c;
                                    String[] stri = {"key", "value"};
                                    MatrixCursor mc = new MatrixCursor(stri);
                                    mc.addRow(new Object[]{str[0], str[1]});
                                    return mc;
                                }
                            }
                        }catch(IOException e2){e2.printStackTrace();}
                    }
                    catch (NullPointerException e1){e1.printStackTrace();}

                }
                catch(NullPointerException e){
                    e.printStackTrace();
                    try {
                        //Log.v("Null Ptr exp", "here");
                        int ji = Integer.parseInt(msgs[port]);
                        Socket nullexsoc=new Socket();
                        nullexsoc.connect(new InetSocketAddress("10.0.2.2", SimpleDynamoActivity.getnode(String.valueOf(ji),1)), 100);
                        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(nullexsoc.getOutputStream()));
                        bw.write(nullexmsg + "\n");
                        bw.flush();
//                        //Log.v("send", nullexmsg);
                        BufferedReader br = new BufferedReader(new InputStreamReader(nullexsoc.getInputStream()));
                        String received_message = br.readLine();
                        String[] str = received_message.split(delim);
                        Cursor c;
                        String[] stri = {"key", "value"};
                        MatrixCursor mc = new MatrixCursor(stri);
                        mc.addRow(new Object[]{str[0], str[1]});
                        return mc;
                    }catch (IOException ew){
                        ew.printStackTrace();
                    }catch (NullPointerException ex){
                        try {
                            //Log.v("Null Ptr exp", "here");
                            int ji = Integer.parseInt(msgs[port]);
                            Socket nullexsoc=new Socket();
                            nullexsoc.connect(new InetSocketAddress("10.0.2.2", SimpleDynamoActivity.getnode(String.valueOf(ji),1)), 100);
                            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(nullexsoc.getOutputStream()));
                            bw.write(nullexmsg + "\n");
                            bw.flush();
//                        //Log.v("send", nullexmsg);
                            BufferedReader br = new BufferedReader(new InputStreamReader(nullexsoc.getInputStream()));
                            String received_message = br.readLine();
                            String[] str = received_message.split(delim);
                            Cursor c;
                            String[] stri = {"key", "value"};
                            MatrixCursor mc = new MatrixCursor(stri);
                            mc.addRow(new Object[]{str[0], str[1]});
                            return mc;
                        }catch (IOException ew){
                            ew.printStackTrace();
                        }catch (NullPointerException x){
                            x.printStackTrace();
                            try {//Log.v("Null Ptr exp", "here");
                                int ji = Integer.parseInt(msgs[port]);
                                Socket nullexsoc = new Socket();
                                nullexsoc.connect(new InetSocketAddress("10.0.2.2", SimpleDynamoActivity.getnode(String.valueOf(ji), 2)), 100);
                                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(nullexsoc.getOutputStream()));
                                bw.write(nullexmsg + "\n");
                                bw.flush();
//                        //Log.v("send", nullexmsg);
                                BufferedReader br = new BufferedReader(new InputStreamReader(nullexsoc.getInputStream()));
                                String received_message = br.readLine();
                                String[] str = received_message.split(delim);
                                Cursor c;
                                String[] stri = {"key", "value"};
                                MatrixCursor mc = new MatrixCursor(stri);
                                mc.addRow(new Object[]{str[0], str[1]});
                                return mc;
                            }catch (IOException z){z.printStackTrace();}
                        }

                    }
                }


                return null;

        }
    }
}