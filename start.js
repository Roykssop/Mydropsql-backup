var bkpCron = require('./backup-cron')( { dbList : 'all', config2 : 'ble'});

console.log("Starting backup",bkpCron);
bkpCron.start();