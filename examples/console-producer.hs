{-# language BangPatterns #-}

import Control.Monad (forM_)

import qualified Fisque.Producer as Producer
import qualified Data.Bytes as Bytes
import qualified Data.Bytes.Chunks as Chunks
import qualified System.IO as IO

main :: IO ()
main = do
  !producer <- Producer.open "."
  bs <- Chunks.hGetContents IO.stdin
  forM_ (Chunks.split 0x0A bs) $ \x -> do
    Producer.bytes producer x
