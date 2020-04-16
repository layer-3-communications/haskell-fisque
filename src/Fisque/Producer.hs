{-# language BangPatterns #-}
{-# language BlockArguments #-}
{-# language DataKinds #-}
{-# language TypeOperators #-}
{-# language LambdaCase #-}
{-# language TypeApplications #-}

module Fisque.Producer
  ( Producer
  , open
  , bytes
  ) where

import Control.Monad.Trans.Except (ExceptT(ExceptT),runExceptT)
import Control.Monad (when)
import Numeric (readHex)
import Control.Exception (throwIO)
import Data.Char (ord)
import Control.Monad.ST.Run (runByteArrayST)
import Data.Word (Word8,Word64)
import System.Directory (listDirectory)
import Data.Primitive (ByteArray,MutablePrimArray)
import Data.Primitive.Unlifted.Array (UnliftedArray)
import GHC.Exts (RealWorld)
import GHC.TypeNats (type (+))
import Data.Bytes (Bytes)
import Foreign.C.String.Managed (ManagedCString(..))
import Foreign.C.Error (Errno)

import qualified Arithmetic.Nat as Nat
import qualified Data.Primitive as PM
import qualified Data.Primitive.Unlifted.Array as PM
import qualified Data.Bytes.Builder.Bounded as BB
import qualified Data.Bytes.Builder.Bounded.Unsafe as Unsafe
import qualified GHC.Exts as Exts
import qualified Posix.File as Posix
import qualified Foreign.C.Error.Describe as Describe

-- | A message producer.
data Producer = Producer
  !ByteArray -- producer base
  !(UnliftedArray ByteArray) -- consumer bases
  !(MutablePrimArray RealWorld Word64)

-- | Open a message producer.
open ::
     FilePath -- ^ base directory
  -> IO Producer
open dir = do
  children <- listDirectory dir
  _ <- case children of
    ["producer","consumer"] -> pure ()
    ["consumer","producer"] -> pure ()
    _ -> fail "Expected producer and consumer directory and nothing else"
  let producerBaseStr = dir ++ "/producer/"
      consumerBaseStr = dir ++ "/consumer/"
  uuid0 <- listDirectory producerBaseStr >>= \case
    [] -> pure 0
    [name] -> case break (=='.') name of
      (s,".fisque") -> case length s of
        12 -> case readHex s of
          [(uuid,"")] -> pure (uuid + 1)
          _ -> fail "expected 48-bit hex file name"
        _ -> fail "expected 12 character file name"
      _ -> fail "expected fisque extension" 
    _ -> fail "bad producer directory"
  mutUuid <- PM.newPrimArray 1
  PM.writePrimArray mutUuid 0 uuid0
  consumers <- listDirectory consumerBaseStr
  let !consumerBases = Exts.fromList $ map (\s -> stringToByteArray (dir ++ "/consumer/" ++ s ++ "/")) consumers
      !producerBase = stringToByteArray producerBaseStr
  pure $! Producer producerBase consumerBases mutUuid

stringToByteArray :: String -> ByteArray
stringToByteArray str = runByteArrayST do
  let !len = length str
  dst <- PM.newByteArray len
  let go !_ [] = pure ()
      go !ix (c:cs) = do
        PM.writeByteArray dst ix (fromIntegral @Int @Word8 (ord c))
        go (ix + 1) cs
  go 0 str
  PM.unsafeFreezeByteArray dst

-- | Enqueue a message. This does not call @fsync@ before returning, so in the
-- event of a system crash, the file corresponding to the message might not
-- be persisted.
bytes :: Producer -> Bytes -> IO ()
bytes (Producer producerBase consumerBases mutUuid) !bs = do
  uuid <- PM.readPrimArray mutUuid 0
  PM.writePrimArray mutUuid 0 (uuid + 1)
  producerPath <- completeBase producerBase uuid
  describeError producerPath $ runExceptT $ do
    fd <- ExceptT (Posix.uninterruptibleOpenMode producerPath Posix.writeOnly (Posix.create <> Posix.exclusive) mempty 0o664)
    ExceptT (Posix.uninterruptibleWriteBytesCompletely fd bs)
    ExceptT (Posix.uninterruptibleClose fd)
  PM.traverseUnliftedArray_
    ( \consumerBase -> do
      consumerPath <- completeBase consumerBase uuid
      describeError2 producerPath consumerPath 
        (Posix.uninterruptibleLink producerPath consumerPath)
    ) consumerBases
  when (uuid > 0) do
    oldPath <- completeBase producerBase (uuid - 1)
    describeError producerPath (Posix.uninterruptibleUnlink oldPath)

describeError :: ManagedCString -> IO (Either Errno a) -> IO a
describeError !path action = action >>= either
  (\e -> fail (Describe.string e ++ " " ++ show path)) pure

describeError2 :: ManagedCString -> ManagedCString -> IO (Either Errno a) -> IO a
describeError2 !path1 !path2 action = action >>= either
  (\e -> fail (Describe.string e ++ " " ++ show path1 ++ " " ++ show path2)) pure

completeBase ::
     ByteArray -- path
  -> Word64 -- uuid
  -> IO ManagedCString
completeBase !producerBase !uuid = do
  let nameLen = Nat.constant @(12 + 7 + 1)
      producerBaseLen = PM.sizeofByteArray producerBase
      producerPathLen = producerBaseLen + Nat.demote nameLen
  producerPath <- PM.newByteArray producerPathLen
  PM.copyByteArray producerPath 0 producerBase 0 producerBaseLen
  Unsafe.pasteIO
    ( BB.word48PaddedLowerHex uuid `BB.append`
      BB.ascii '.' `BB.append`
      BB.ascii 'f' `BB.append`
      BB.ascii 'i' `BB.append`
      BB.ascii 's' `BB.append`
      BB.ascii 'q' `BB.append`
      BB.ascii 'u' `BB.append`
      BB.ascii 'e' `BB.append`
      BB.word8 0x00
    ) producerPath producerBaseLen
  r <- PM.unsafeFreezeByteArray producerPath
  pure (ManagedCString r)
