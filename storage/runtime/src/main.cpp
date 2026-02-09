#include "common.h"

#include <cstdio>

int main() {
    const int msg_id = MessageQueueDispatcher::OpenQueue(MSG_KEY, IPC_CREAT | 0660);
    if (msg_id < 0) {
        std::perror("[BPE] msgget");
        return 1;
    }

    SharedMemorySlots shm_slots;
    MessageQueueDispatcher dispatcher(msg_id, shm_slots.CreateWorkers());
    dispatcher.Run();
    return 0;
}
