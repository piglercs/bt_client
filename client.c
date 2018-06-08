#include<stdio.h> //printf
#include<string.h>    //strlen
#include<sys/socket.h>    //socket
#include<arpa/inet.h> //inet_addr
#include<stdlib.h>

#define BT_HOST "10.230.137.234"
#define BT_PORT 1555

int get_socket_connection(){
    int sock;
    struct sockaddr_in server;

    //Create socket
    sock = socket(AF_INET , SOCK_STREAM , 0);

    if (sock == -1)
    {
        return sock;
    }

    server.sin_addr.s_addr = inet_addr(BT_HOST);
    server.sin_family = AF_INET;
    server.sin_port = htons(BT_PORT);

    connect(sock , (struct sockaddr *)&server , sizeof(server));

    if (connect(sock , (struct sockaddr *)&server , sizeof(server)) < 0)
    {
        return sock;
    }

    return sock;
}

int readFromServer(int sock, char** server_reply){

    char * header =  malloc(sizeof(char)* 10);

    recv(sock, header, 10, 0);
    recv(sock, *server_reply, 32000, 0);

    free(header);
    return 1;

}

int send2server(int sock, char* message){

    unsigned char  header[2];
    int msg_len = strlen(message);
    header[0] = (msg_len / 255) & 0xFF;
    header[1] = (msg_len % 255) & 0xFF;

    send(sock , header , 2, 0);
    send(sock , message , msg_len, 0);

    return 1;
}

char* send_and_receive(int sock, char* message){

    char * server_reply = malloc(sizeof(char)* 32000);

    send2server(sock, message);
    readFromServer(sock, &server_reply);

    return server_reply;
}

int verify(int sock, char* message){
    char * server_reply = malloc(sizeof(char)* 32000);
    send(sock , message , strlen(message), 0);
    recv(sock , server_reply , 32000 , 0);
    
    if(strcmp(server_reply, "+ARCOK") == 0){
        free(server_reply);
	return 0;
    }else{
        free(server_reply);
	return 1;
    }
}

int init_connection(char* username){
    int socket = get_socket_connection();
    if(socket <0){
        return socket;
    }else{
        if(verify(socket, "ARCDS") != 0){
	    return -1;
	}else{
 	    char login_command[50];
	    strcpy(login_command,"login|-u|");
            strcat(login_command, username);
            strcat(login_command, ",fin_gpdb_dev");
	    char* entry_command = "arcentry| |10.230.136.94,3307,10.230.136.94, ,fin_gpdb_dev,10.230.136.94,QueryIntercept";
	    char* db_change_command = "arcdsqchange|-i|fin_gpdb_dev,finance";
	    char* schema_change_command = "arcschchange|-i|fin_gpdb_dev,finance,analytics";
	    char * result = send_and_receive(socket,login_command);
            free(result);
	    result = send_and_receive(socket,entry_command);
	    free(result);
	    result = send_and_receive(socket,db_change_command);
	    free(result);
	    result = send_and_receive(socket,schema_change_command);
	    free(result);
            // if all ok
            return socket;
	}
    }
}

char* send_query(int socket, char* query){
    char* flag =  "arcsql| |";
    char message_with_flag[strlen(query) + strlen(flag)];
    strcpy(message_with_flag, flag);
    strcat(message_with_flag, query);
    
    return send_and_receive(socket, message_with_flag);
}

int close_socket(int sock){
    shutdown(sock, SHUT_RDWR); // todo add error handling
    return 0;
}

int main(){

    int s = init_connection("200123456");
    
    char* response = send_query(s, "SELECT * FROM xx_gl_balances LIMIT 10;");
    printf("Response: %s\n", response);
    free(response);   
     
    close_socket(s);
    return 0;
}
