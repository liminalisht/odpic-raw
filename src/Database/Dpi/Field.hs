{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE TupleSections        #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeSynonymInstances #-}
module Database.Dpi.Field(
    DataField(..)
  , FromDataFields(..)
  , FromDataField(..)
  , ToDataField(..)
  , isNullable
  , getName
  , fromByteString
  , toByteString
  , toDataFieldMap
  , readDataField
  , DataFieldMap
  ) where

import           Database.Dpi.Internal
import           Database.Dpi.Internal as I (Data_DataTypeInfo(oracleTypeNum))
import           Database.Dpi.Prelude
import           Database.Dpi.Util

import           Control.Exception      (throw)
import           Data.ByteString        (ByteString)
import qualified Data.ByteString        as B
import qualified Data.ByteString.Char8  as BC
import qualified Data.ByteString.Unsafe as B
import           Data.Int               (Int64)
import           Data.Monoid            ((<>))
import           Data.Time

-- | Database Raw Data with Type Info
data DataField = DataField
  { info  :: !Data_QueryInfo -- ^ Type Info
  , value :: !DataValue      -- ^ Raw Value
  } deriving (Show)


-- | Check if data field is nullable
{-# INLINE isNullable #-}
isNullable    :: DataField -> Bool
isNullable DataField{..} = let Data_QueryInfo{..} = info in nullOk

{-# INLINE getName #-}
getName :: DataField -> IO ByteString
getName DataField{..} = let Data_QueryInfo{..} = info in tsLen name

type DataFieldMap = [(ByteString, DataField)]

toDataFieldMap :: [DataField] -> IO DataFieldMap
toDataFieldMap = mapM go
  where
    go f = (,f) <$> getName f

readDataField :: FromDataField a => DataFieldMap -> ByteString -> IO (Maybe a)
readDataField dfm name = case lookup name dfm of
  Nothing -> return Nothing
  Just v  -> fromDataField v

-- | Some types can convert from 'DataField'
class FromDataField a where
  fromDataField  :: DataField -> IO (Maybe a) --todo: should this be Either?

instance FromDataField Bool where
  fromDataField DataField{..} = let Data_QueryInfo{..} = info in case value of
    (DataNull    _) -> pure Nothing
    (DataBoolean v) -> pure $ Just v
    _ -> singleError' name

instance FromDataField ByteString where
  fromDataField DataField{..} = let Data_QueryInfo{..} = info in case value of
    (DataNull  _) -> pure Nothing
    (DataBytes v) -> fmap Just $ toByteString v
    _ -> singleError' name

instance FromDataField Int64 where
  fromDataField DataField{..} = let Data_QueryInfo{..} = info in case value of
    (DataNull _) -> pure Nothing
    (DataInt  v) -> pure . Just $ v
    _ -> singleError' name

instance FromDataField Word64 where
  fromDataField DataField{..} = let Data_QueryInfo{..} = info in case value of
    (DataNull  _) -> pure Nothing
    (DataUint  v) -> pure . Just $ v
    _ -> singleError' name

instance FromDataField Double where
  fromDataField DataField{..} = let Data_QueryInfo{..} = info in case value of
    (DataNull             _) -> pure Nothing
    (DataDouble (CDouble v)) -> pure . Just $ v
    _ -> singleError' name

instance FromDataField Float where
  fromDataField DataField{..} = let Data_QueryInfo{..} = info in case value of
    (DataNull             _) -> pure Nothing
    (DataFloat (CFloat   v)) -> pure . Just $ v
    _ -> singleError' name

-- all these time related functions are suspect
instance FromDataField UTCTime where
  fromDataField DataField{..} =
    let Data_QueryInfo{..} = info
    in case (value, I.oracleTypeNum typeInfo) of
      (DataNull          _,                      _) -> pure Nothing
      (DataTimestamp     v,         OracleTypeDate) -> pure . Just $ toUTCTime  v
      (DataTimestamp     v,    OracleTypeTimestamp) -> pure . Just $ toUTCTime  v
      (DataTimestamp     v, OracleTypeTimestampLtz) -> pure . Just $ toUTCTimeL v
      (DataTimestamp     v,  OracleTypeTimestampTz) -> pure . Just $ toUTCTime  v
      _                        -> singleError' name

-- all these time related functions are suspect
instance FromDataField ZonedTime where
  fromDataField DataField{..} =
    let Data_QueryInfo{..} = info
    in case (value, I.oracleTypeNum typeInfo) of
      (DataNull          _,                      _) -> pure Nothing
      (DataTimestamp     v,         OracleTypeDate) -> pure . Just $ toZonedTime False v
      (DataTimestamp     v,    OracleTypeTimestamp) -> pure . Just $ toZonedTime False v
      (DataTimestamp     v, OracleTypeTimestampLtz) -> pure . Just $ toZonedTime True  v
      (DataTimestamp     v,  OracleTypeTimestampTz) -> pure . Just $ toZonedTime False v
      _                        -> singleError' name

-- all these time related functions are suspect
instance FromDataField LocalTime where
  fromDataField = fmap (fmap go) . fromDataField
    where
      go ZonedTime{..} = zonedTimeToLocalTime

-- | Some types can convert to 'DataValue'
class ToDataField a where
  toDataField :: a -> IO DataValue

instance ToDataField ByteString where
  toDataField = fmap DataBytes . fromByteString

instance ToDataField Bool where
  toDataField = pure . DataBoolean

instance ToDataField Int64 where
  toDataField = pure . DataInt . fromIntegral

instance ToDataField Word64 where
  toDataField = pure . DataUint . fromIntegral

instance ToDataField Double where
  toDataField = pure . DataDouble . CDouble

instance ToDataField Float where
  toDataField = pure . DataFloat . CFloat

-- all these time related functions are suspect
instance ToDataField UTCTime where
  toDataField = pure . DataTimestamp . fromUTCTime

-- all these time related functions are suspect
instance ToDataField ZonedTime where
  toDataField = pure . DataTimestamp . fromZonedTime

fromByteString :: ByteString -> IO Data_Bytes
fromByteString bs = B.unsafeUseAsCStringLen bs $ \bytes -> let encoding ="utf-8" in return Data_Bytes{..}

-- | Convert from CStringLen to ByteString
toByteString :: Data_Bytes -> IO ByteString
toByteString Data_Bytes{..} = B.packCStringLen bytes

{-# INLINE fromDiffTime #-}
fromDiffTime :: DiffTime -> Data_IntervalDS
fromDiffTime dt =
  let dts           = diffTimeToPicoseconds dt
      (r1,fseconds) = dts `divMod` pico
      (r2,seconds)  = r1  `divMod` 60
      (r3,minutes)  = r2  `divMod` 60
      (days,hours)  = r3  `divMod` 24
  in Data_IntervalDS (fromInteger days) (fromInteger hours) (fromInteger minutes) (fromInteger seconds) (fromInteger fseconds)

pico :: Integer
pico = (10 :: Integer) ^ (12 :: Integer)

{-# INLINE toDiffTime #-}
toDiffTime :: Data_IntervalDS -> DiffTime
toDiffTime Data_IntervalDS{..} = picosecondsToDiffTime $ toInteger fseconds + pico * (toInteger seconds + 60 * (toInteger minutes + 60 * (toInteger hours + 24 * toInteger days)))

{-# INLINE fromUTCTime #-}
fromUTCTime :: UTCTime -> Data_Timestamp
fromUTCTime UTCTime{..} =
  let (year,month,day)    = toGregorian utctDay
      Data_IntervalDS{..} = fromDiffTime utctDayTime
  in Data_Timestamp (fromInteger year) (fe month) (fe day) (fe hours) (fe minutes) (fe seconds) (fe fseconds) 0 0

{-# INLINE toUTCTime #-}
toUTCTime :: Data_Timestamp -> UTCTime
toUTCTime = zonedTimeToUTC . toZonedTime False

{-# INLINE toUTCTimeL #-}
toUTCTimeL :: Data_Timestamp -> UTCTime
toUTCTimeL = zonedTimeToUTC . toZonedTime True

{-# INLINE fromZonedTime #-}
fromZonedTime :: ZonedTime -> Data_Timestamp
fromZonedTime ZonedTime{..} =
  let timestamp    = fromUTCTime (localTimeToUTC utc zonedTimeToLocalTime)
      TimeZone{..} = zonedTimeZone
      (h,m)        = timeZoneMinutes `divMod` 60
  in  timestamp { tzHourOffset = fe h, tzMinuteOffset = fe m }

{-# INLINE toZonedTime #-}
toZonedTime :: Bool -> Data_Timestamp -> ZonedTime
toZonedTime isLocal Data_Timestamp{..} =
  let utctDay     = fromGregorian (fe year) (fe month) (fe day)
      days        = 0
      hours       = fe hour
      minutes     = fe minute
      seconds     = fe second
      fseconds    = fe fsecond
      utctDayTime = toDiffTime Data_IntervalDS{..}
      offset      = toInteger tzHourOffset * 60 + toInteger tzMinuteOffset
      timezone    = minutesToTimeZone $ fromInteger offset
  in if isLocal
       then utcToZonedTime timezone $ addUTCTime (fromInteger $ 60 * negate offset) UTCTime{..}
       else utcToZonedTime timezone $ addUTCTime (fromInteger $ 60 * negate offset) UTCTime{..}

{-# INLINE singleError #-}
singleError :: String -> IO a
singleError name = throw $ DpiException $ "type mismatch " <> name

{-# INLINE singleError' #-}
singleError' :: CStringLen -> IO a
singleError' name = B.packCStringLen name >>= singleError . BC.unpack

class FromDataFields a where
  fromDataFields'  :: [DataField]  -> IO a
  fromDataFields' dfs = toDataFieldMap dfs >>= fromDataFields
  fromDataFields :: DataFieldMap -> IO a
  fromDataFields dfm = fromDataFields' $ fmap snd dfm
  {-# MINIMAL fromDataFields | fromDataFields' #-}

instance FromDataFields [DataField] where
  fromDataFields' = return
