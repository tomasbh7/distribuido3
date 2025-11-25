#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <stdbool.h>
#include <string.h>

#define ELECTION_MSG 1
#define ANSWER_MSG 2
#define COORDINATOR_MSG 3
#define ELECTION_TIMEOUT 3.0

typedef struct {
    int rank;
    int size;
    bool alive;
    bool coordinator;
    int leader;
    bool participant;
    bool in_election;
    double election_start_time;
} NodeState;

// Configuración de nodos caídos
bool is_node_failed(int rank) {
    int failed_nodes[] = {2, 4}; //Estos son ejemplos de los nodos caídos, lo podemos modificar
    for (int i = 0; i < sizeof(failed_nodes)/sizeof(failed_nodes[0]); i++) {
        if (rank == failed_nodes[i]) {
            return true;
        }
    }
    return false;
}

// Función que filtra por tipo de mensaje esperado
bool receive_with_timeout_filtered(int* message, int count, double timeout_seconds, int expected_msg_type) {
    MPI_Request request;
    MPI_Status status;
    int flag = 0;
    double start_time = MPI_Wtime();
    
    MPI_Irecv(message, count, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &request);
    
    while (MPI_Wtime() - start_time < timeout_seconds) {
        MPI_Test(&request, &flag, &status);
        if (flag) {
            // Checamos que el mensaje es del tipo esperado
            if (message[1] == expected_msg_type) {
                return true;
            } else {
                // Mensaje no esperado - reencolar y continuar espera
                printf("Nodo %d: Mensaje inesperado tipo %d, ignorando\n", status.MPI_SOURCE, message[1]);
                // Reiniciar recepción
                MPI_Irecv(message, count, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &request);
            }
        }
        // Espera activa corta
        double current = MPI_Wtime();
        while (MPI_Wtime() - current < 0.001) {}
    }
    
    MPI_Cancel(&request);
    MPI_Request_free(&request);
    return false;
}

// Función para procesar mensajes COORDINATOR inmediatamente
bool process_coordinator_messages(NodeState* node) {
    int buffer[2];
    MPI_Status status;
    int flag = 0;
    MPI_Request request;
    
    MPI_Irecv(&buffer, 2, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &request);
    MPI_Test(&request, &flag, &status);
    
    if (flag && buffer[1] == COORDINATOR_MSG) {
        printf("Nodo %d: Recibi COORDINATOR de %d\n", node->rank, buffer[0]);
        node->leader = buffer[0];
        node->coordinator = (node->rank == buffer[0]);
        node->in_election = false;
        node->participant = false;
        printf("Nodo %d: Acepto nuevo lider: %d\n", node->rank, node->leader);
        return true;
    }
    
    MPI_Cancel(&request);
    return false;
}

// Verificar si hay nodos con mayor ID vivos
bool has_higher_alive_nodes(NodeState* node) {
    for (int i = node->rank + 1; i < node->size; i++) {
        if (!is_node_failed(i)) {
            return true;
        }
    }
    return false;
}

// Nodos caídos NO envían mensajes
void send_to_alive_node(int dest, int* message, int count) {
    if (!is_node_failed(dest)) {
        MPI_Send(message, count, MPI_INT, dest, 0, MPI_COMM_WORLD);
    }
}

//  Función para declararse líder con envío confirmado
void declare_leader(NodeState* node) {
    node->coordinator = true;
    node->leader = node->rank;
    node->in_election = false;
    node->participant = false;
    
    printf("Nodo %d: Me declaro lider (timeout alcanzado)\n", node->rank);
    
    // Envío multiple para garantizar que todos reciban el mensaje
    int buffer[2] = {node->rank, COORDINATOR_MSG};
    for (int retry = 0; retry < 3; retry++) {
        for (int i = 0; i < node->size; i++) {
            if (i != node->rank && !is_node_failed(i)) {
                printf("Nodo %d: Enviando coordinador a %d (intento %d)\n", node->rank, i, retry + 1);
                send_to_alive_node(i, buffer, 2);
            }
        }
        // Pequeña pausa entre reintentos
        double current = MPI_Wtime();
        while (MPI_Wtime() - current < 0.1) {}
    }
}


void bully_algorithm(NodeState* node) {
    if (!node->alive) {
        printf("Nodo %d: CAIDO - no participa en comunicacion\n", node->rank);
        return;
    }
    
    printf("Nodo %d: VIVO - Lider actual: %d\n", node->rank, node->leader);
    
    // El nodo 1 inicia la elección
    if (node->rank == 1) {
        sleep(2); // Dar tiempo a que todos inicialicen
        
        printf("\nNodo %d: Iniciando eleccion - TIMEOUT: %.1f segundos\n", node->rank, ELECTION_TIMEOUT);
        
        node->in_election = true;
        node->participant = true;
        node->election_start_time = MPI_Wtime();
        
        while (node->in_election) {
            // Verificar constantemente mensajes COORDINATOR
            if (process_coordinator_messages(node)) {
                break;
            }
            
            // Verificar si soy el mayor ID vivo
            if (!has_higher_alive_nodes(node)) {
                printf("Nodo %d: Soy el mayor ID vivo\n", node->rank);
                declare_leader(node);
                break;
            }
            
            // Ronda de comunicación con TIMEOUT REAL
            printf("Nodo %d: Enviando elección a nodos mayores\n", node->rank);
            int election_msg[2] = {node->rank, ELECTION_MSG};
            int responses_expected = 0;
            
            for (int i = node->rank + 1; i < node->size; i++) {
                if (!is_node_failed(i)) {
                    printf("Nodo %d: -> Elección a %d\n", node->rank, i);
                    send_to_alive_node(i, election_msg, 2);
                    responses_expected++;
                }
            }
            
            if (responses_expected == 0) {
                printf("Nodo %d: No hay nodos mayores vivos\n", node->rank);
                declare_leader(node);
                break;
            }
            
            // ESPERA CON TIMEOUT REAL filtrado solo para ANSWER
            printf("Nodo %d: Esperando respuesta (timeout real: %.1fs)\n", node->rank, ELECTION_TIMEOUT);
            bool received_answer = false;
            double wait_start = MPI_Wtime();
            
            while ((MPI_Wtime() - wait_start < ELECTION_TIMEOUT)) {
                // Procesar COORDINATOR primero
                if (process_coordinator_messages(node)) {
                    node->in_election = false;
                    return;
                }
                
                int response[2];
                // SOLO aceptar mensajes ANSWER durante esta espera
                if (receive_with_timeout_filtered(response, 2, 0.1, ANSWER_MSG)) {
                    printf("Nodo %d: <- Respuesta de %d\n", node->rank, response[0]);
                    received_answer = true;
                    node->participant = false;
                    break;
                }
            }
            
            // Decisiónm  basada en timeout real
            if (!received_answer) {
                printf("Nodo %d: TIMEOUT - No obtuve respuesta de nodos mayores\n", node->rank);
                declare_leader(node);
                break;
            } else {
                // Esperar COORDINATOR con timeout
                printf("Nodo %d: Esperando coordinador...\n", node->rank);
                double coord_wait_start = MPI_Wtime();
                while ((MPI_Wtime() - coord_wait_start < ELECTION_TIMEOUT)) {
                    if (process_coordinator_messages(node)) {
                        break;
                    }
                    double current = MPI_Wtime();
                    while (MPI_Wtime() - current < 0.05) {}
                }
                break;
            }
        }
    }
    
    // Procesamiento para TODOS los nodos
    double start_time = MPI_Wtime();
    while (MPI_Wtime() - start_time < 15.0) {
        // Procesar ELECTION messages
        int buffer[2];
        if (receive_with_timeout_filtered(buffer, 2, 0.1, ELECTION_MSG)) {
            printf("Nodo %d: Recibi eleccion de %d\n", node->rank, buffer[0]);
            
            if (node->alive && !node->coordinator) {
                // Responder inmediatamente
                int answer[2] = {node->rank, ANSWER_MSG};
                printf("Nodo %d: Enviando respuesta a %d\n", node->rank, buffer[0]);
                send_to_alive_node(buffer[0], answer, 2);
                
                // Iniciar propia elección
                if (!node->in_election) {
                    printf("Nodo %d: Iniciando mi eleccion\n", node->rank);
                    node->in_election = true;
                    
                    if (!has_higher_alive_nodes(node)) {
                        declare_leader(node);
                    } else {
                        int my_election[2] = {node->rank, ELECTION_MSG};
                        for (int i = node->rank + 1; i < node->size; i++) {
                            if (!is_node_failed(i)) {
                                printf("Nodo %d: -> eleccion a %d\n", node->rank, i);
                                send_to_alive_node(i, my_election, 2);
                            }
                        }
                    }
                }
            }
        }
    
        process_coordinator_messages(node);
        
        double current = MPI_Wtime();
        while (MPI_Wtime() - current < 0.05) {}
    }
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    NodeState node;
    node.rank = rank;
    node.size = size;
    node.alive = !is_node_failed(rank);
    node.coordinator = false;
    node.leader = size - 1;
    node.participant = false;
    node.in_election = false;
    node.election_start_time = 0.0;
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    if (rank == 0) {
        printf("\n=== ALGORITMO BULLY ===\n");
        printf("Configuracion: %d nodos | Caidos: ", size);
        for (int i = 0; i < size; i++) {
            if (is_node_failed(i)) printf("%d ", i);
        }
        printf("\n=================================================\n\n");
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    bully_algorithm(&node);
    
    MPI_Barrier(MPI_COMM_WORLD);
    sleep(2);
    
    printf("Nodo %d: REPORTE FINAL - ", node.rank);
    if (node.alive) {
        printf("VIVO, Lider: %d", node.leader);
        if (node.coordinator) printf(" (SOY_LIDER)");
    } else {
        printf("CAIDO");
    }
    printf("\n");

    if (node.alive) {
        int my_leader = node.leader;
        int leaders[size];
        
        MPI_Gather(&my_leader, 1, MPI_INT, leaders, 1, MPI_INT, 0, MPI_COMM_WORLD);
        
        if (rank == 0) {
            printf("\n=== VERIFICACION FINAL DE CONSISTENCIA ===\n");
            
            int consensus_leader = -1;
            bool consistent = true;
            
            for (int i = 0; i < size; i++) {
                if (!is_node_failed(i)) {
                    printf("Nodo %d -> Lider: %d\n", i, leaders[i]);
                    if (consensus_leader == -1) {
                        consensus_leader = leaders[i];
                    } else if (leaders[i] != consensus_leader) {
                        consistent = false;
                        printf("ERROR: Inconsistencia en nodo %d\n", i);
                    }
                }
            }
            
            if (consistent) {
                printf("EXITO: Todos los nodos vivos acordaron el lider %d\n", consensus_leader);
            } else {
                printf("FALLA: No hay consenso en el lider\n");
            }
        }
    } else {
        int dummy = -1;
        MPI_Gather(&dummy, 1, MPI_INT, NULL, 1, MPI_INT, 0, MPI_COMM_WORLD);
    }
    
    MPI_Finalize();
    return 0;
}