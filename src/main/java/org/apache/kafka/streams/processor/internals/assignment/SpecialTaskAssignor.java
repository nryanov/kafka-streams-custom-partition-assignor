package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;

import java.util.*;

public class SpecialTaskAssignor implements TaskAssignor {
    @Override
    public boolean assign(Map<UUID, ClientState> clients, Set<TaskId> allTaskIds, Set<TaskId> statefulTaskIds, AssignorConfiguration.AssignmentConfigs assignmentConfigs) {
        // Так как мы точно знаем, что у всех топиков одинаковое число партиций,
        // то мы можем сгруппировать таски по номерам партиций.
        // Каждый таск при этом -- это юнит чтения данных из конкретной партиции топика
        var tasksPerPartition = new HashMap<Integer, Set<TaskId>>();

        allTaskIds.forEach(task -> tasksPerPartition.compute(task.partition, (ignored, set) -> {
            if (set == null) {
                set = new HashSet<>();
            }

            set.add(task);
            return set;
        }));

        var clientsSize = clients.size();

        // Собираем список клиентов. Клиент != consumer
        var clientList = new ArrayList<UUID>();
        clients.forEach((uuid, state) -> clientList.add(uuid));

        // Каждому клиенту по принципу RoundRobin назначаем таски, привязанные к конкретной партиции
        var currentClient = 0;
        for (var tasks : tasksPerPartition.entrySet()) {
            var client = clients.get(clientList.get(currentClient % clientsSize));

            tasks.getValue().forEach(client::assignActive);
            currentClient++;
        }

        // Дополнительная ребалансировка не нужна, поэтому всегда возвращаем false
        return false;
    }
}
