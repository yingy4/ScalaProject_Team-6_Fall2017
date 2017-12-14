
create table `testHouses` (

  `Id` INT NOT NULL ,
  `YearBuilt` INT NOT NULL,
  `YearRemodelled` INT NOT NULL,
  `NumberOfBedrooms` INT NOT NULL,
  `NumberOfBathrooms` DOUBLE NOT NULL,
  `NumberOfFloors` DOUBLE NOT NULL,
  `LivingArea` INT NOT NULL,
  `BasementExisting` INT NOT NULL,
  `FireplaceExisting` INT NOT NULL,
  `PoolExisting` INT NOT NULL,
  `GarageExisting` INT NOT NULL,
  `GarageCarCount` INT NOT NULL,
  `CloseToCommute` INT NOT NULL,
  `SalesCondition` INT NOT NULL,
  `ZipCode` INT NOT NULL,
  `SalesPrice` TEXT

);


create table `Users` (
  `idUser` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `first_name` TEXT NOT NULL,
  `last_name` TEXT NOT NULL,
  `mobile` BIGINT NOT NULL,
  `email` TEXT NOT NULL,
  `sellerBuyer` TEXT
);

