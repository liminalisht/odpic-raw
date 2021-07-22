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
import           Data.Scientific
import           Data.Time
import           Data.Time.Clock.POSIX

-- | Database Raw Data with Type Info
data DataField = DataField
  { info  :: !Data_QueryInfo -- ^ Type Info
  , value :: !DataValue      -- ^ Raw Value
  } deriving (Show)

-- | Some Type can convert from 'DataField'
class FromDataField a where
  fromDataField  :: DataField -> IO (Maybe a)

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

instance FromDataField ByteString where
  fromDataField DataField{..} = let Data_QueryInfo{..} = info in go name typeInfo value
    where
      go _ _ (DataNull          _) = return Nothing
      go _ _ (DataBytes         v) = Just <$> toByteString v
      go n _ _                     = singleError' n

instance FromDataField Integer where
  fromDataField = fmap (fmap (round :: Scientific -> Integer)) . fromDataField

instance FromDataField Int where
  fromDataField = fmap (fmap fromInteger) . fromDataField

instance FromDataField Int64 where
  fromDataField = fmap (fmap fromInteger) . fromDataField

instance FromDataField Word where
  fromDataField = fmap (fmap fromInteger) . fromDataField

instance FromDataField Word64 where
  fromDataField = fmap (fmap fromInteger) . fromDataField

instance FromDataField Double where
  fromDataField = fmap (fmap (realToFrac :: Scientific -> Double)) . fromDataField

instance FromDataField Float where
  fromDataField = fmap (fmap (realToFrac :: Scientific -> Float)) . fromDataField

instance FromDataField Scientific where
  fromDataField DataField{..} = let Data_QueryInfo{..} = info in go name typeInfo value
    where
      go _ _ (DataNull          _) = return Nothing
      go _ _ (DataInt           v) = return . Just $ realToFrac v
      go _ _ (DataUint          v) = return . Just $ realToFrac v
      go _ _ (DataNumInt        v) = return . Just $ realToFrac v
      go _ _ (DataNumUint       v) = return . Just $ realToFrac v
      go _ _ (DataFloat         v) = return . Just $ realToFrac v
      go _ _ (DataNumDouble     v) = return . Just $ realToFrac v
      go _ _ (DataDouble        v) = return . Just $ realToFrac v
      go _ _ (DataBytes Data_Bytes{..}) = (Just . read . BC.unpack) <$> tsLen bytes
      go n _ _                     = singleError' n

instance FromDataField Bool where
  fromDataField DataField{..} = let Data_QueryInfo{..} = info in go name typeInfo value
    where
      go _ _ (DataNull          _) = return Nothing
      go _ _ (DataBoolean       v) = return $ Just v
      go n _ _                     = singleError' n

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
      -- go _ _ (DataIntervalDs    v) = !Data_IntervalDS
      -- go _ _ (DataIntervalYm    v) = !Data_IntervalYM

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
      -- go _ _ (DataIntervalDs    v) = !Data_IntervalDS
      -- go _ _ (DataIntervalYm    v) = !Data_IntervalYM

instance FromDataField LocalTime where
  fromDataField = fmap (fmap go) . fromDataField
    where
      go ZonedTime{..} = zonedTimeToLocalTime

instance FromDataField DiffTime where
  fromDataField DataField{..} = let Data_QueryInfo{..} = info in go name typeInfo value
    where
      go _ _ (DataNull          _) = return Nothing
      go _ _ (DataIntervalDs    v) = return . Just $ toDiffTime  v
      go _ _ (DataIntervalYm    v) = return . Just $ toDiffTime' v
      go n _ _                     = singleError' n

-- | Some type can convert to 'DataValue'
class ToDataField a where
  toDataField :: a -> NativeTypeNum -> OracleTypeNum -> IO DataValue

instance ToDataField ByteString where
  toDataField v NativeTypeBytes OracleTypeChar        = fmap DataBytes $ fromByteString v
  toDataField v NativeTypeBytes OracleTypeLongRaw     = fmap DataBytes $ fromByteString v
  toDataField v NativeTypeBytes OracleTypeLongVarchar = fmap DataBytes $ fromByteString v
  toDataField v NativeTypeBytes OracleTypeNchar       = fmap DataBytes $ fromByteString v
  toDataField v NativeTypeBytes OracleTypeNumber      = fmap DataBytes $ fromByteString v
  toDataField v NativeTypeBytes OracleTypeNvarchar    = fmap DataBytes $ fromByteString v
  toDataField v NativeTypeBytes OracleTypeRaw         = fmap DataBytes $ fromByteString v
  toDataField v NativeTypeBytes OracleTypeVarchar     = fmap DataBytes $ fromByteString v
  toDataField _ _               _                     = singleError     "Text"

instance ToDataField Bool where
  toDataField v NativeTypeBoolean OracleTypeBoolean = return $ DataBoolean v
  toDataField _ _                 _                 = singleError "Bool"

instance ToDataField Integer where
  toDataField v NativeTypeDouble OracleTypeNativeDouble = return $ DataDouble    $ realToFrac v
  toDataField v NativeTypeDouble OracleTypeNumber       = return $ DataNumDouble $ realToFrac v
  toDataField v NativeTypeFloat  OracleTypeNativeFloat  = return $ DataFloat     $ realToFrac v
  toDataField v NativeTypeInt64  OracleTypeNativeInt    = return $ DataInt       $ fromIntegral v
  toDataField v NativeTypeInt64  OracleTypeNumber       = return $ DataNumInt    $ fromIntegral v
  toDataField v NativeTypeUint64 OracleTypeNativeUint   = return $ DataUint      $ fromIntegral v
  toDataField v NativeTypeUint64 OracleTypeNumber       = return $ DataNumUint   $ fromIntegral v
  toDataField _ _                 _                     = singleError "Integer"

instance ToDataField Int where
  toDataField v = toDataField (toInteger v)

instance ToDataField Int64 where
  toDataField v = toDataField (toInteger v)

instance ToDataField Word where
  toDataField v = toDataField (toInteger v)

instance ToDataField Word64 where
  toDataField v = toDataField (toInteger v)

instance ToDataField Scientific where
  toDataField v NativeTypeDouble OracleTypeNativeDouble = return $ DataDouble    $ realToFrac v
  toDataField v NativeTypeDouble OracleTypeNumber       = return $ DataNumDouble $ realToFrac v
  toDataField v NativeTypeFloat  OracleTypeNativeFloat  = return $ DataFloat     $ realToFrac v
  toDataField v NativeTypeInt64  OracleTypeNativeInt    = return $ DataInt       $ round v
  toDataField v NativeTypeInt64  OracleTypeNumber       = return $ DataNumInt    $ round v
  toDataField v NativeTypeUint64 OracleTypeNativeUint   = return $ DataUint      $ round v
  toDataField v NativeTypeUint64 OracleTypeNumber       = return $ DataNumUint   $ round v
  toDataField _ _                 _                     = singleError "Decimal"

instance ToDataField Double where
  toDataField v NativeTypeDouble OracleTypeNativeDouble = return $ DataDouble    $ realToFrac v
  toDataField v NativeTypeDouble OracleTypeNumber       = return $ DataNumDouble $ realToFrac v
  toDataField v NativeTypeFloat  OracleTypeNativeFloat  = return $ DataFloat     $ realToFrac v
  toDataField v NativeTypeInt64  OracleTypeNativeInt    = return $ DataInt       $ round v
  toDataField v NativeTypeInt64  OracleTypeNumber       = return $ DataNumInt    $ round v
  toDataField v NativeTypeUint64 OracleTypeNativeUint   = return $ DataUint      $ round v
  toDataField v NativeTypeUint64 OracleTypeNumber       = return $ DataNumUint   $ round v
  toDataField _ _                 _                     = singleError "Double"

instance ToDataField Float where
  toDataField v = toDataField (realToFrac v :: Double)

instance ToDataField UTCTime where
  toDataField v NativeTypeTimestamp OracleTypeDate         = return $ DataTimestamp     $ fromUTCTime  v
  toDataField v NativeTypeTimestamp OracleTypeTimestamp    = return $ DataTimestamp     $ fromUTCTime  v
  toDataField v NativeTypeTimestamp OracleTypeTimestampLtz = return $ DataTimestamp     $ fromUTCTime  v
  toDataField v NativeTypeTimestamp OracleTypeTimestampTz  = return $ DataTimestamp     $ fromUTCTime  v
  toDataField _ _                 _                        = singleError "UTCTime"

instance ToDataField ZonedTime where
  toDataField v NativeTypeTimestamp OracleTypeDate         = return $ DataTimestamp     $ fromZonedTime v
  toDataField v NativeTypeTimestamp OracleTypeTimestamp    = return $ DataTimestamp     $ fromZonedTime v
  toDataField v NativeTypeTimestamp OracleTypeTimestampLtz = return $ DataTimestamp     $ fromZonedTime v
  toDataField v NativeTypeTimestamp OracleTypeTimestampTz  = return $ DataTimestamp     $ fromZonedTime v
  toDataField _ _                 _                        = singleError "ZonedTime"

instance ToDataField DiffTime where
  toDataField v NativeTypeIntervalDs OracleTypeIntervalDs = return $ DataIntervalDs $ fromDiffTime  v
  toDataField v NativeTypeIntervalYm OracleTypeIntervalYm = return $ DataIntervalYm $ fromDiffTime' v
  toDataField _ _                 _                       = singleError "DiffTime"

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

{-# INLINE fromDiffTime' #-}
fromDiffTime' :: DiffTime -> Data_IntervalYM
fromDiffTime' dt =
  let dts   = diffTimeToPicoseconds dt `div` (30 * 86400 * pico)
      (y,m) = dts `divMod` 12
  in Data_IntervalYM (fromInteger y) (fromInteger m)

{-# INLINE toDiffTime #-}
toDiffTime :: Data_IntervalDS -> DiffTime
toDiffTime Data_IntervalDS{..} = picosecondsToDiffTime $ toInteger fseconds + pico * (toInteger seconds + 60 * (toInteger minutes + 60 * (toInteger hours + 24 * toInteger days)))

{-# INLINE toDiffTime' #-}
toDiffTime' :: Data_IntervalYM -> DiffTime
toDiffTime' Data_IntervalYM{..} = secondsToDiffTime $ 30 * 86400 * pico * (toInteger years * 12 + toInteger months)

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

-- instance FromDataFields String where
--   fromDataFields' dfs = intercalate "," <$> sequence (fmap go dfs)
--     where
--       go  f@DataField{..} = go2 f value
--       go3 f = (fromMaybe "" . fmap show) <$> f
--       go2 v (DataBoolean       _) = go3 (fromDataField v :: IO (Maybe Bool)       )
--       go2 v (DataInt           _) = go3 (fromDataField v :: IO (Maybe Integer)    )
--       go2 v (DataNumInt        _) = go3 (fromDataField v :: IO (Maybe Integer)    )
--       go2 v (DataNumUint       _) = go3 (fromDataField v :: IO (Maybe Integer)    )
--       go2 v (DataUint          _) = go3 (fromDataField v :: IO (Maybe Integer)    )
--       go2 v (DataDouble        _) = go3 (fromDataField v :: IO (Maybe Double)     )
--       go2 v (DataNumDouble     _) = go3 (fromDataField v :: IO (Maybe Double)     )
--       go2 v (DataFloat         _) = go3 (fromDataField v :: IO (Maybe Float)      )
--       go2 v (DataIntervalDs    _) = go3 (fromDataField v :: IO (Maybe DiffTime)   )
--       go2 v (DataIntervalYm    _) = go3 (fromDataField v :: IO (Maybe DiffTime)   )
--       go2 v (DataTimestampLtzD _) = go3 (fromDataField v :: IO (Maybe ZonedTime)  )
--       go2 v (DataTimestampTzD  _) = go3 (fromDataField v :: IO (Maybe ZonedTime)  )
--       go2 v (DataBytes         _) = go3 (fromDataField v :: IO (Maybe ByteString) )
--       go2 v (DataTimestamp     _) = go3 (fromDataField v :: IO (Maybe UTCTime)    )
--       go2 _ _                     = return ""
