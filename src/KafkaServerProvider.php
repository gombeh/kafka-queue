<?php
namespace Gombeh\KafkaQueue;

use Illuminate\Support\ServiceProvider;

class KafkaServerProvider extends ServiceProvider
{
    /**
     * Bootstrap services.
     *
     * @return void
     */
    public function boot()
    {
        $manager = $this->app['queue'];

        $manager->addConnector('kafka', function () {
            return new KafkaConnector;
        });
    }
}
