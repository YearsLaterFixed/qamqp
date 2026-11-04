#include <qamqpclient.h>

int main()
{
    QAmqpClient client;
    return client.isConnected() ? 1 : 0;
}