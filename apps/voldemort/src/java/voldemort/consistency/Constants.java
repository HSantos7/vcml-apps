package voldemort.consistency;


import voldemort.consistency.cluster.Node;
import voldemort.consistency.versioning.Version;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

public class Constants {
    static int maxMetadataRefreshAttempts = 10;
    static long putOpTimeoutInMs = 5000;
    static long getOpTimeoutInMs = 5000;
    static long deleteOpTimeoutInMs = 5000;
    static int requiredWrites = 1;
    static int preferredWrites = 1;
    static int requiredZones = 0;
    static int requiredReads = 1;
    static int preferedReads = 1;
    static boolean repairReads = true;
    public static String versioning = "voldemort.consistency.versioning.VectorClock";
    public static String key = "java.lang.String";
    private static String value = "java.lang.String";
    static HashMap<Integer, Boolean> permissions = new HashMap<Integer, Boolean>(){{ //pair Node/timestamper
        put(0,true);
    }};

    public static String communicationlocal = "voldemort.consistency.CommunicationLocal";
    public static String communicationremote = "voldemort.consistency.CommunicationRemote";
    public static String groupMembership = "voldemort.consistency.GroupMembership";
    public static String quorum = "voldemort.consistency.Quorum";
    public static String order = "voldemort.consistency.Order";
    public static String deliverycondition = "voldemort.consistency.DeliveryCondition";
    public static String replicate = "voldemort.consistency.Replicate";


    //nao modificar
    private static Class<?> versionType = null;
    private static Class<?> keyType = null;
    private static Class<?> valueType = null;

    public static <K,V> ReplicateInterface<K,V> getReplicateClass(GroupMembershipInterface groupMembership, QuorumInterface<K,V> quorum, CommunicationInterface.internal<K,V> internal, CommunicationInterface.external<K,V> external, DeliveryConditionInterface<K,V> deliveryCondition) {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(replicate);
            Class[] params = new Class[] {  GroupMembershipInterface.class, QuorumInterface.class, CommunicationInterface.internal.class, CommunicationInterface.external.class, DeliveryConditionInterface.class};
            Constructor<?> constr = clazz.getConstructor(params);
            return (ReplicateInterface <K,V>) constr.newInstance(groupMembership, quorum, internal, external, deliveryCondition);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <K,V> DeliveryConditionInterface<K,V> getDeliveryConditionClass(OrderInterface<K,V> orderInterface, CommunicationInterface.internal<K,V> internal, CommunicationInterface.external<K,V> external, GroupMembershipInterface groupMembership) {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(deliverycondition);
            Class[] params = new Class[] { OrderInterface.class, CommunicationInterface.internal.class, CommunicationInterface.external.class, GroupMembershipInterface.class};
            Constructor<?> constr = clazz.getConstructor(params);
            return (DeliveryConditionInterface <K,V>) constr.newInstance(orderInterface, internal, external, groupMembership);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <K,V> OrderInterface<K,V> getOrderClass(int max, GroupMembershipInterface groupMembership, CommunicationInterface.internal<K,V> internal) {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(order);
            Class[] params = new Class[] { int.class, GroupMembershipInterface.class, CommunicationInterface.internal.class};
            Constructor<?> constr = clazz.getConstructor(params);
            return (OrderInterface <K,V>) constr.newInstance(max, groupMembership, internal);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static <K,V> GroupMembershipInterface getGroupMembershipClass(Node classArg) {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(groupMembership);
            Class[] params = new Class[] { Node.class};
            Constructor<?> constr = clazz.getConstructor(params);
            return (GroupMembershipInterface) constr.newInstance(classArg);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <K,V> QuorumInterface<K,V> getQuorumClass() {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(quorum);
            Class[] params = new Class[] {};
            Constructor<?> constr = clazz.getConstructor(params);
            return (QuorumInterface<K,V>) constr.newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <K,V> CommunicationInterface.external<K,V> getCommunicationRemoteClass(GroupMembershipInterface classArg) {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(communicationremote);
            Class[] params = new Class[] { GroupMembershipInterface.class};
            Constructor<?> constr = clazz.getConstructor(params);
            return (CommunicationInterface.external<K,V>) constr.newInstance(classArg);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <K,V> CommunicationInterface.internal<K,V> getCommunicationLocalClass(GroupMembershipInterface classArg) {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(communicationlocal);
            Class[] params = new Class[] { GroupMembershipInterface.class};
            Constructor<?> constr = clazz.getConstructor(params);
            return (CommunicationInterface.internal<K,V>) constr.newInstance(classArg);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Class<?> getVersionType() {
        try {
            if (versionType == null){
                ClassLoader classLoader = Constants.class.getClassLoader();
                versionType = classLoader.loadClass(versioning);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return versionType;
    }

    public static Class<?> getKeyType() {
        try {
            if (keyType == null){
                ClassLoader classLoader = Constants.class.getClassLoader();
                keyType = classLoader.loadClass(key);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return keyType;
    }

    public static Class<?> getValueType() {
        try {
            if (valueType == null){
                ClassLoader classLoader = Constants.class.getClassLoader();
                valueType = classLoader.loadClass(value);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return valueType;
    }

    public static Version getVersionObject(){
        Class<? extends Version> classDefinition = null;
        try {
            classDefinition = (Class<? extends Version>) Class.forName(Constants.versioning);
            Constructor<? extends Version> cons = classDefinition .getConstructor();
            return cons.newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
            return null;
        }
    }
}
