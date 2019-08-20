using Dakata.Example.Models;
namespace Dakata.Example.Dal
{
    public class PurchaseOrderDal : BaseDal<PurchaseOrder>
    {
        public PurchaseOrderDal(DapperConnection connection): base(connection)
        {
        }
    }
}
