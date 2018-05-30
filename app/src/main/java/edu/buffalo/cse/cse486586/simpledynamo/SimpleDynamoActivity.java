package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;


public class SimpleDynamoActivity extends Activity {
static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    public static int numbr_of_nodes=1;
    public static String my_avd_hash=null;
    public static String delim="!@#%&";
    int port=0;//Index of port argument in input of client task
    static String myPort;
    int message=1;//Index of messaage argument in input string of client task
    public  static List<String> hashed_avds_list = new ArrayList<String>();
    public  static HashMap<String,String>hash_to_port= new HashMap<String, String>();
    public  static List<String>topology = new ArrayList<String>();
    public static String nxt_node=null;
    public static String prev_node=null;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        my_avd_hash=SimpleDynamoProvider.genHash(portStr);
        //Display my port number and avd number
        String strReceived = "my port is "+myPort+"I am avd "+portStr;
        TextView remoteTextView = (TextView) findViewById(R.id.textView1);
        remoteTextView.append(strReceived + "\t\n");
        TextView localTextView = (TextView) findViewById(R.id.textView1);
        localTextView.append("\n");
        //Start Server
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            //Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        //fill up lookup data structures
        for(int i=5554;i<=5562;i+=2){
            hashed_avds_list.add(SimpleDynamoProvider.genHash(Integer.toString(i)));
            hash_to_port.put(SimpleDynamoProvider.genHash(Integer.toString(i)),Integer.toString(i*2));
        }
        Collections.sort(hashed_avds_list);
        topology.clear();
        topology.addAll(hash_to_port.keySet());
        Collections.sort(topology);
        numbr_of_nodes=topology.size();


        nxt_node=SimpleDynamoProvider.genHash(String.valueOf(getnode(myPort,1)/2));
        prev_node=SimpleDynamoProvider.genHash(String.valueOf(getnode(myPort,-1)/2));

        //Log.v("hardcoded ",topology.toString()+" prev "+prev_node+" me "+SimpleDynamoProvider.genHash(portStr)+" nxt adb -s emulator-5562 pull /data/data/edu.buffalo.cse.cse486586.simpledynamo/databases/kvalpair.db C:\\Users\n"+nxt_node);

        //register yourself to the ring
        //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,REMOTE_PORT0,"request to join ring my portnumber is"+delim+myPort);
        //Gimme missed writes
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"gimme_missed_writes");


    }
private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            try {
                //Log.v("server up","running at port"+myPort);
                while (true) {
                    Socket accepted_socket = serverSocket.accept();
                    BufferedReader br = new BufferedReader(new InputStreamReader(accepted_socket.getInputStream()));
                    String received_message = br.readLine();

                    if (received_message != null){
                        //Log.v("server",received_message);
                        String [] str=received_message.split(delim);
                         if(str[0].equals("insert")){
                            ContentValues value=new ContentValues();
                            value.put("key",str[1]);
                            value.put("value",str[2]);
                           //force insert
                            SimpleDynamoProvider.myDb.insert(value,1);
                            ////Log.v("server","force insert");
                             insert_in_next_n(str[1], str[2],2);
                        }else if(str[0].equals("insert_replica")){
                             ContentValues value=new ContentValues();
                             value.put("key",str[1]);
                             value.put("value",str[2]);
                             SimpleDynamoProvider.myDb.insert(value,Integer.parseInt(find_owner(str[1])));
                             //Log.v("server","insert replica");
                         }
                         else if(str[0].equals("insert_backup")){
                             ContentValues value=new ContentValues();
                             value.put("key",str[1]);
                             value.put("value",str[2]);
                             SimpleDynamoProvider.myDb.insert(value,Integer.parseInt(find_owner(str[1])));
                             //Log.v("server","insert backup");
                             //insert_in_next_n(str[1], str[2],1);
                         }
                         else if(str[0].equals("gimme_missed_writes")) {
                             String remote_port=str[1];
                             Cursor c=SimpleDynamoProvider.myDb.query_response(str[1],Integer.parseInt(remote_port));
                             String keys=delim;
                             String values=delim;
                             c.moveToFirst();
                             while(!c.isAfterLast()){
                                 keys+=c.getString(c.getColumnIndex("key"))+delim;
                                 values+=c.getString(c.getColumnIndex("value"))+delim;
                                 c.moveToNext();
                             }
                             BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(accepted_socket.getOutputStream()));
                             bw.write(keys+"\n");
                             //bw.flush();
                             bw.write(values+"\n");
                             bw.flush();
                             //Log.v("answerin gme_msed_wrts"+myPort,"keys "+keys+"values "+values);
                         }

                         else if(str[0].equals("query")){
                            Cursor c=SimpleDynamoProvider.myDb.query_response(str[1],1);
                             if(c != null && c.moveToFirst()) {
                                 try {
                                        c.moveToFirst();
                                        int keyIndex = c.getColumnIndex("key");
                                        int valueIndex = c.getColumnIndex("value");

                                        String returnKey = c.getString(keyIndex);
                                        String returnValue = c.getString(valueIndex);
                                        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(accepted_socket.getOutputStream()));
                                        bw.write(returnKey + delim + returnValue + "\n");
                                        bw.flush();
                                }catch (CursorIndexOutOfBoundsException e){
                                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(accepted_socket.getOutputStream()));
                                    bw.write(null + "\n");
                                    bw.flush();
                                     }
                             }//else{
//                                 BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(accepted_socket.getOutputStream()));
//                                 bw.write(null + "\n");
//                                 bw.flush();
//                             }

                        }else if(str[0].equals("delete")){
                             SimpleDynamoProvider.myDb.delete("*");
                         }else if(str[0].equals("*")){
                            Cursor c=SimpleDynamoProvider.myDb.query_response("@",0);
                            String keys=delim;
                            String values=delim;
                            c.moveToFirst();
                            while(!c.isAfterLast()){
                              keys+=c.getString(c.getColumnIndex("key"))+delim;
                              values+=c.getString(c.getColumnIndex("value"))+delim;
                              c.moveToNext();
                            }
                            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(accepted_socket.getOutputStream()));
                            bw.write(keys+"\n");
                            //bw.flush();
                            bw.write(values+"\n");
                            bw.flush();
                        }
                    }
                    accepted_socket.close();
                    br.close();
                    //Log.v("topology",topology.toString()+" nodes "+numbr_of_nodes+" prev "+prev_node+" me "+my_avd_hash+" next "+nxt_node);
                }

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private void brodcast_new_topology() {
        String msg="updated topology"+delim;
        for(String i:topology){
            msg+=i+delim;
        }
        try {
            Socket socket;
            for(int ji =Integer.parseInt(REMOTE_PORT0);ji<=11124;ji=ji+4){
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),ji);

                String msgToSend = msg;
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                bw.write(msgToSend+"\n");
                bw.flush();
                //socket.close();
            }

        } catch (UnknownHostException e) {
            //Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            //Log.e(TAG, "ClientTask socket IOException");
        }
    }
    public static int getnode(String prt, int i) {
        //create hash of port find pos in topology file return port of the prt+ith port in the ring using topology list


        try {
            List<String> top =new ArrayList<String>(topology);
            int prt_index = top.indexOf(SimpleDynamoProvider.genHash(String.valueOf(Integer.parseInt(prt) / 2)));
            String p = null;
            if (prt_index + i < 0) {
                //Log.v("getnode", "got:" + p + "input" + prt + "," + String.valueOf(i) + "," + String.valueOf((prt_index + i) % top.size()));
                p = top.get(prt_index + i + top.size());
            } else
                p = top.get((prt_index + i) % (top.size()));
            //Log.v("getnode", "got:" + p + "input" + prt + "," + String.valueOf(i) + "," + String.valueOf((prt_index + i) % top.size()));
            int port = Integer.parseInt(hash_to_port.get(p));
            return port;
        }
        catch (IndexOutOfBoundsException e){
            List<String> t=new ArrayList<String>();
            for(int li=5554;li<=5562;li+=2){
                t.add(SimpleDynamoProvider.genHash(Integer.toString(li)));
            }
            int prt_index = t.indexOf(SimpleDynamoProvider.genHash(String.valueOf(Integer.parseInt(prt) / 2)));
            String p = null;
            if (prt_index + i < 0) {
                //Log.v("getnode", "got:" + p + "input" + prt + "," + String.valueOf(i) + "," + String.valueOf((prt_index + i) % t.size()+t.toString()));
                p = t.get(prt_index + i + t.size());
            } else
                p = t.get((prt_index + i) % (t.size()));
            //Log.v("getnode", "got:" + p + "input" + prt + "," + String.valueOf(i) + "," + String.valueOf((prt_index + i) % t.size()));
            int port = Integer.parseInt(hash_to_port.get(p));
            return port;
        }

    }

    private void insert_in_next_n(String key, String value,int n) {//inserted already in this nodes db now insert in db of next 2 nodes
        //stale copy with the node that dies so it asks for new copy from next2 nodes
        //while querying * return only the keys in nodes orignal range as @ will also give the backup copies
        //create write in next1 which inserts in own db and in db of next node
        try{

            for(int i =1;i<=n;i++){
                int ji=getnode(myPort,i);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),ji);

                String msgToSend ="insert_replica"+delim+key+delim+value ;
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                bw.write(msgToSend+"\n");
                bw.flush();

            }
        }catch (IOException e){}

    }
    private String find_owner(String key) {
        List<String>position_finder = new ArrayList<String>(topology);
        position_finder.add(SimpleDynamoProvider.genHash(key));
        Collections.sort(position_finder);
        int i= position_finder.indexOf(SimpleDynamoProvider.genHash(key));
        if(i==position_finder.size()-1)
            i=0;
        else
            i++;
        return hash_to_port.get(position_finder.get(i));
    }



    protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            TextView localTextView = (TextView) findViewById(R.id.textView1);
            localTextView.append("\n");
        }
        private class ClientTask extends AsyncTask<String, Void, Void> {

            @Override
            protected Void doInBackground(String... msgs) {
                if(msgs[0].equals("gimme_missed_writes")){
                    try {

                        for (int ka = 1; ka <= 2; ka++) {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), getnode(myPort, ka));//ask from me+2
                            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                            bw.write("gimme_missed_writes" + delim + myPort + "\n");
                            bw.flush();
                            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String keys = br.readLine();
                            //Log.v("got_missed_writes", keys);
                            String values = br.readLine();
                            //Log.v("got_missed_writes", values);
                            String[] k = keys.split(delim);
                            String[] v = values.split(delim);
                            for (int i = 0; i < k.length; i++) {
                                ContentValues value = new ContentValues();
                                if(Integer.parseInt(find_owner(k[i]))==Integer.parseInt(myPort)) {
                                    value.put("key", k[i]);
                                    value.put("value", v[i]);
                                    //force insert
                                    SimpleDynamoProvider.myDb.insert(value, 1);
                                }
                            }

                        }
                        for (int ka = 1; ka <= 2; ka++) {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), getnode(myPort, -ka));//ask from me-2
                            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                            bw.write("gimme_missed_writes" + delim + myPort + "\n");
                            bw.flush();
                            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String keys = br.readLine();
                            //Log.v("got_missed_writes", keys);
                            String values = br.readLine();
                            //Log.v("got_missed_writes", values);
                            String[] k = keys.split(delim);
                            String[] v = values.split(delim);
                            for (int i = 0; i < k.length; i++) {
                                ContentValues value = new ContentValues();
                                if(Integer.parseInt(find_owner(k[i]))==getnode(myPort,-1)||Integer.parseInt(find_owner(k[i]))==getnode(myPort,-2)) {
                                    value.put("key", k[i]);
                                    value.put("value", v[i]);
                                    //force insert
                                    SimpleDynamoProvider.myDb.insert(value, 1);
                                }
                            }

                        }

                    }catch (UnknownHostException e) {
                        e.printStackTrace();
                    }catch (IOException e){e.printStackTrace();
                    }catch (NullPointerException e){e.printStackTrace();}

                }

//
                return null;
            }
        }


	

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    //Log.v("Test", "onStop()");
	}

}
