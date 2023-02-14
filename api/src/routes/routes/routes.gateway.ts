import { Inject, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import {
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Producer } from 'kafkajs';
import { Server, Socket } from 'socket.io';

@WebSocketGateway({ cors: true })
export class RoutesGateway implements OnModuleInit {
  private kafkaProducer: Producer;

  @WebSocketServer()
  server: Server;

  constructor(@Inject('KAFKA_SERVICE') private clientKafka: ClientKafka) {}

  async onModuleInit() {
    this.kafkaProducer = await this.clientKafka.connect();
  }

  @SubscribeMessage('new-direction')
  handleMessage(client: Socket, payload: { routeId: string }): void {
    this.kafkaProducer.send({
      topic: 'route.new-direction',
      messages: [
        {
          key: 'route.new-direction',
          value: JSON.stringify({
            routeId: payload.routeId,
            clientId: client.id,
          }),
        },
      ],
    });
    console.log(payload);
  }

  sendPosition(data: {
    clientId: string;
    routeId: string;
    position: [number, number];
    finished: boolean;
  }) {
    const { clientId, ...rest } = data;
    const clients = this.server.sockets.sockets;
    const currentClient = clients.get(clientId);
    if (!currentClient) {
      console.error(
        'Client not exists, refresh React App and resend new direction again',
      );
      return;
    }
    currentClient.emit('new-position', rest);
  }
}
