
#include "txn/storage.h"

bool Storage::Read(Key key, Value* result, int txn_unique_id)
{
    if (data_.count(key))
    {
        *result = data_[key];
        return true;
    }
    else
    {
        return false;
    }
}

// Write value and timestamps
void Storage::Write(Key key, Value value, int txn_unique_id)
{
    data_[key]       = value;
    timestamps_[key] = GetTime();
}

double Storage::Timestamp(Key key)
{
    if (timestamps_.count(key) == 0) return 0;
    return timestamps_[key];
}

// Init the storage
void Storage::InitStorage()
{
    for (int i = 0; i < 1000000; i++)
    {
        Write(i, 0, 0);
    }
}
