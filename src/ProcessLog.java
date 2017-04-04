import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
/**
 * Created by swapnil on 4/1/2017.
 * swapnilk@ksu.edu
 */

public class ProcessLog {
    final static String dateFormat = "dd/MMM/yyyy:HH:mm:ss Z";
    private static Map<String, Long> hostMap = new HashMap<>();
    private static Map<String, Long> resourceMap = new HashMap<>();
    private static Map<String, BlockHost> blockMap = new HashMap<>();
    private static List<String> blockList = new ArrayList<>();
    private static Map<Date, Long> dateMap = new HashMap<>(3);
    private static PriorityQueue<TimeCount> dateQueue = new PriorityQueue<>();
    private static int dateQueueCount = 1;
    private static Date logEndDate;
    private static Date logStartDate;
    private static boolean isStartDateSet = false;

    // Container to store Date and count for priorityqueue
    private static class TimeCount implements Comparable<TimeCount>{
        Date time;
        long count;

        public TimeCount(Date time, long count){
            this.time = time;
            this.count = count;
        }

        public int compareTo(TimeCount other){
            return (this.count == other.count) ? other.time.compareTo(this.time):Long.compare(this.count,other.count);
        }

        public String toString(){
            return convertDateToString(time)+","+count;
        }
    }



//    Details of blocked host for logging into blocked.txt file
    private static class BlockHost{
        Date startTime;
        int count;
        Date tillBlockTime;

        public BlockHost(Date startTime, int count){
            this.startTime = startTime;
            this.count = count;
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startTime);
            calendar.set(Calendar.SECOND,(calendar.get(Calendar.SECOND)+20));
            this.tillBlockTime = calendar.getTime();
        }
    }

    // Container to store Host and count for priorityqueue
    private static class HostCount implements Comparable<HostCount>{
        private String name;
        private long count;

        public HostCount(String name, long count){
            this.name = name;
            this.count = count;
        }

        public int compareTo(HostCount other){
            return (this.count == other.count) ? this.name.compareTo(other.name):Long.compare(this.count,other.count);
        }

        public String toString(){
            return name+","+count+"\n";
        }
    }

    // Container to store Resource and count for priorityqueue
    private static class ResourceCount implements Comparable<ResourceCount>{
        private String name;
        private long count;

        public ResourceCount(String name, long count){
            this.name = name;
            this.count = count;
        }

        public int compareTo(ResourceCount other){
            return (this.count == other.count) ? this.name.compareTo(other.name):Long.compare(this.count,other.count);
        }

        public String toString(){
            return name+","+count+"\n";
        }
    }

    // Write aggregated blocked host to the file
    private static void processBlockedList(String fileName){
        try(BufferedWriter br = new BufferedWriter(new FileWriter(fileName))){
            for(String item : blockList){
                br.write(item+"\n");
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    // Process hashmap to find top 10 hosts
    private static void processHostMap(String fileName){
        List<HostCount> list = new LinkedList<>();
        PriorityQueue<HostCount> queue = new PriorityQueue<>();
        int count = 1;
        for(Map.Entry<String, Long> entry : hostMap.entrySet()){
            if(count <= 10){
                queue.offer(new HostCount(entry.getKey(), entry.getValue()));
                count++;
            }else{
                queue.offer(new HostCount(entry.getKey(), entry.getValue()));
                queue.poll();
            }
        }
        while(!queue.isEmpty()){
            list.add(0,queue.poll());
        }

        try(BufferedWriter br = new BufferedWriter(new FileWriter(fileName))){
            for(HostCount h : list){
                br.write(h.toString());
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    // Process hashmap to find top 10 resources
    private static void processResourceMap(String fileName){
        List<String> list = new LinkedList<>();
        PriorityQueue<ResourceCount> queue = new PriorityQueue<>();
        int count = 1;
        for(Map.Entry<String, Long> entry : resourceMap.entrySet()){
            if(count <= 10){
                queue.offer(new ResourceCount(entry.getKey(), entry.getValue()));
                count++;
            }else{
                queue.offer(new ResourceCount(entry.getKey(), entry.getValue()));
                queue.poll();
            }
        }
        while(!queue.isEmpty()){
            list.add(0,queue.poll().name);
        }

        try(BufferedWriter br = new BufferedWriter(new FileWriter(fileName))){
            for(String item : list){
                br.write(item+"\n");
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    // Process remaining entries in the hashmap to find top 10 busy hours
    private static void process60minutes(String fileName){
        List<TimeCount> list = new LinkedList<>();
        Iterator<Map.Entry<Date, Long>> iter = dateMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Date, Long> entry = iter.next();
            if (dateQueueCount <= 10) {
                dateQueue.offer(new TimeCount(entry.getKey(), entry.getValue()));
                iter.remove();
                dateQueueCount++;
            } else {
                dateQueue.offer(new TimeCount(entry.getKey(), entry.getValue()));
                iter.remove();
                dateQueue.poll();
            }
        }

        while(!dateQueue.isEmpty()){
            list.add(0, dateQueue.poll());
        }

        try(BufferedWriter br = new BufferedWriter(new FileWriter(fileName))){
            for(TimeCount h : list){
                br.write(h.toString()+"\n");
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    // Main function to read the file for input values
    private static void readLogFile(String fileName) {

        try(BufferedReader br = new BufferedReader(new FileReader(fileName))){
            String line = br.readLine();
            while (line != null) {
                line = line.trim();
                if(line.equals("")) {
                    line = br.readLine();
                    continue;
                }
                String[] lineSplit= line.split("- -");
                lineSplit[1] = lineSplit[1].trim();
                String time = lineSplit[1].substring(1,27);
                Date dateTime = convertStringToDate(time);
                if(!isStartDateSet) {
                    logStartDate = dateTime;
                    isStartDateSet = true;
                }
                if(dateTime == null) {
                    line = br.readLine();
                    continue;
                }
                String host = lineSplit[0].trim();
                String rest = lineSplit[1].substring(30);
                int index = rest.indexOf("\"");
                if(index == -1) {
                    line = br.readLine();
                    continue;
                }
                String request = rest.substring(0,index);
                String requestSplit[] = request.split("\\s+");
                String requestType = requestSplit[0];
                String requestResource = requestSplit[1];
                rest = rest.substring(index+2);
                String status = rest.substring(0,3);
                String bytes = rest.substring(4).equals("-") ? "0":rest.substring(4);
                processTimeRequest(dateTime);
                if(requestResource.equals("/login")){
                    processCurrentLogin(line, dateTime, host, status);
                }
                hostMap.put(host,hostMap.getOrDefault(host, 0l)+1);
                resourceMap.put(requestResource, resourceMap.getOrDefault(requestResource, 0l) + Long.parseLong(bytes));
                line = br.readLine();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    // Sliding window with dictionary to find busiest hour
    private static void processTimeRequest(Date startTime){
        Date key = null;
        for(int i = 0; i < 3600; i++){
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startTime);
            calendar.set(Calendar.SECOND,(calendar.get(Calendar.SECOND)-i));
            key = calendar.getTime();
            if(!key.before(logStartDate))
                dateMap.put(key, dateMap.getOrDefault(key, 0l)+1);
        }
        Iterator<Map.Entry<Date, Long>> iter = dateMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Date, Long> entry = iter.next();
            if(entry.getKey().before(key))
            {
                if(dateQueueCount <= 10){
                    dateQueue.offer(new TimeCount(entry.getKey(), entry.getValue()));
                    iter.remove();
                    dateQueueCount++;
                }else{
                    dateQueue.offer(new TimeCount(entry.getKey(), entry.getValue()));
                    iter.remove();
                    dateQueue.poll();
                }

            }
        }

    }

    // Process to block user after 3 consecutive failed attempts from 5 mins of third attempt
    private static void processCurrentLogin(String line, Date dateTime, String host, String status){

        if(blockMap.containsKey(host)){
            BlockHost blocked = blockMap.get(host);
            if(dateTime.after(blocked.tillBlockTime)){
                if(!status.equals("200")){
                    BlockHost temp = new BlockHost(dateTime, 1);
                    blockMap.put(host, temp);
                }else{
                    blockMap.remove(host);
                }
            }else{
                if(blocked.count == 1){
                    if(status.equals("200")){
                        blockMap.remove(host);
                    }else{
                        blocked.count += 1;
                    }
                }
                else if(blocked.count == 2){
                    if(status.equals("200")){
                        blockMap.remove(host);
                    }else{
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime(blocked.startTime);
                        calendar.set(Calendar.SECOND,(calendar.get(Calendar.SECOND)+300));
                        blocked.tillBlockTime = calendar.getTime();
                        blocked.count += 1;
                    }
                }
                else if(blocked.count >= 3){
                    blocked.count += 1;
                    blockList.add(line);
                }
            }
        }else if(!status.equals("200")){
            BlockHost temp = new BlockHost(dateTime, 1);
            blockMap.put(host, temp);
        }
    }

    // Convert a string to date object
    private static Date convertStringToDate(String date){
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        Date dt = null;
        try{
            dt = sdf.parse(date);
        }catch (ParseException e){
            e.printStackTrace();
        }
        return dt;
    }

//    Convert date to string for after adding 1 hour to battle day light saving
    private static String convertDateToString(Date date){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.SECOND,(calendar.get(Calendar.SECOND)+3600));
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss -0400");
        String dt = sdf.format(calendar.getTime());
        return dt;
    }

    // Driver function
    public static void main(String[] args){
        String logFile = args[0];
        String hostFile = args[1];
        String hoursFile = args[2];
        String resourceFile = args[3];
        String blockedFile = args[4];

        readLogFile(logFile);
        processHostMap(hostFile);
        processResourceMap(resourceFile);
        processBlockedList(blockedFile);
        process60minutes(hoursFile);
    }
}