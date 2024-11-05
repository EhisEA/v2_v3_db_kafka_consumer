<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Junges\Kafka\Contracts\ConsumerMessage;
use Junges\Kafka\Contracts\MessageConsumer;
use Junges\Kafka\Facades\Kafka;

class ProcessCdcEvents extends Command
{
    protected $signature = 'cdc:process';
    protected $description = 'Consume messages from Kafka topics';

    public function handle()
    {
        // Create a consumer for the specific topic(s)
        $consumer = Kafka::consumer(['v2_postgres_db.public.users', 'v3_mysql_db.users'])
        ->withConsumerGroupId('1')
            ->withHandler(function(ConsumerMessage $message, MessageConsumer $consumer) {
                // Process the received message
                $this->processMessage($message);
            })
            ->build();

        // Start consuming messages
        $consumer->consume();
    }

    protected function processMessage( $message)
    {
        // Decode the message body
        $data = json_decode($message->getBody(), true);

        // Log the received message for debugging
        $this->info('Received message: ' . json_encode($data));

        // Here you can implement your data transformation and conflict resolution logic
        $this->handleDataTransformation($data);
    }

    protected function handleDataTransformation(array $data)
    {
        // Implement your data transformation logic here
        // For example, inserting or updating records in your MySQL database

        if (isset($data['after'])) {
            // Assuming 'after' contains the new data to be inserted or updated
            $userData = $data['after'];

            // Use Laravel's Eloquent or DB facade to perform database operations
            \DB::table('users')->updateOrInsert(
                ['id' => $userData['id']], // Assuming 'id' is the unique identifier
                $userData // The data to insert or update
            );
        }
    }
}
